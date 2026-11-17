# Gerenciador Distribuído de Tarefas — GDT

> Trabalho acadêmico de Redes de Computadores: aplicação distribuída no modelo cliente-servidor, implementada em Python sobre sockets TCP.

## 1. Identificação do trabalho

| Campo | Informação |
|---|---|
| Aplicação | Gerenciador Distribuído de Tarefas (GDT) |
| Arquitetura | Cliente-servidor |
| Linguagem | Python 3.10 ou superior |
| Transporte | TCP sobre IPv4 ou IPv6, conforme resolução do endereço |
| Protocolo de aplicação | GDT/1.0, criado para este projeto |
| Formato das mensagens | JSON em UTF-8, delimitado por `LF` (`\n`) |
| Porta padrão | TCP 5050 |
| Dependências externas | Nenhuma |

**Aluno(a):** preencher antes da entrega  
**Disciplina/turma:** preencher antes da entrega  
**Professor(a):** preencher antes da entrega  
**Data:** preencher antes da entrega

## 2. Visão geral

O GDT é um quadro de tarefas compartilhado. Um servidor mantém o estado central e aceita simultaneamente diversos clientes conectados pela rede. Cada cliente pode consultar, criar, concluir e excluir tarefas. Quando uma alteração ocorre, todos os clientes identificados recebem um evento assíncrono; assim, a aplicação demonstra comunicação bidirecional e concorrência, em vez de apenas uma sequência isolada de pergunta e resposta.

O propósito é oferecer um exemplo pequeno, executável e bem documentado dos conceitos fundamentais de sistemas distribuídos: sockets, protocolo da camada de aplicação, enquadramento de mensagens, estados de conexão, correlação entre requisição e resposta, concorrência, sincronização de dados e persistência.

### 2.1 Funcionalidades

- identificação do cliente por nome de usuário;
- criação de tarefas com título e descrição;
- listagem do quadro compartilhado;
- conclusão e exclusão de tarefas por identificador;
- notificação assíncrona de mudanças para todos os clientes conectados;
- teste de disponibilidade por `PING`/`PONG`;
- persistência atômica do quadro em arquivo JSON;
- validação de entrada e erros de aplicação padronizados;
- atendimento concorrente por uma thread para cada conexão;
- testes automatizados unitários e de integração.

## 3. Arquitetura e funcionamento

```text
                         TCP / GDT 1.0
┌──────────────┐       ┌──────────────────────────────┐
│ Cliente A    │◄─────►│                              │
│ terminal/CLI │       │  Servidor concorrente        │
└──────────────┘       │  - máquina de estados        │
                       │  - validação do protocolo    │
┌──────────────┐       │  - armazenamento sincronizado│       ┌────────────┐
│ Cliente B    │◄─────►│  - difusão de eventos        │◄─────►│ tasks.json │
│ terminal/CLI │       │                              │       └────────────┘
└──────────────┘       └──────────────────────────────┘
```

O arquivo `server.py` abre um socket de escuta e usa `ThreadingTCPServer`, da biblioteca padrão, para atender conexões em paralelo. Todas as threads compartilham um `TaskStore`; um bloqueio reentrante (`RLock`) impede condições de corrida durante alterações. A gravação persistente é feita primeiro em arquivo temporário e depois substituída com `os.replace`, reduzindo o risco de arquivo parcialmente escrito.

O arquivo `client.py` possui uma interface de linha de comando e uma classe `TaskClient`. Uma thread receptora permanece lendo o socket. Respostas são entregues à requisição correspondente pelo campo `request_id`, enquanto mensagens `EVENT` seguem para uma fila separada. Essa separação permite ao servidor enviar notificações a qualquer momento.

O arquivo `protocol.py` contém as constantes, a serialização, o enquadramento e as validações comuns. Compartilhar esse módulo reduz divergências entre as duas implementações do protocolo.

## 4. Escolha do protocolo de transporte

Foi escolhido **TCP** porque as operações alteram um estado compartilhado e não podem ser perdidas, duplicadas ou processadas fora de ordem. O TCP fornece:

- entrega confiável, ordenada e sem duplicação dos bytes;
- controle de fluxo e congestionamento;
- conexão persistente, adequada para eventos enviados pelo servidor;
- detecção do encerramento da outra extremidade.

Com UDP seria necessário implementar confirmação, retransmissão, ordenação, deduplicação e controle de fragmentação na própria aplicação. Isso aumentaria a complexidade sem trazer vantagem relevante para mensagens pequenas e interativas. O TCP, entretanto, transporta um **fluxo de bytes**, não mensagens. Por esse motivo, o GDT/1.0 define explicitamente seu próprio enquadramento por quebra de linha.

## 5. Protocolo da camada de aplicação GDT/1.0

### 5.1 Convenções de transporte e enquadramento

1. O cliente inicia uma conexão TCP com o servidor.
2. Cada quadro é um objeto JSON compacto, codificado em UTF-8 e terminado por um byte `LF` (`0x0A`).
3. Não é permitido inserir um `LF` literal dentro de uma string JSON; a serialização o representa como `\n`.
4. O tamanho máximo de um quadro, incluindo o delimitador, é 65.536 bytes.
5. Várias mensagens podem trafegar na mesma conexão.
6. A conexão fica aberta até `QUIT`, fim do fluxo, erro irrecuperável, tempo limite ou falha de rede.
7. Palavras de tipo são representadas em maiúsculas. O servidor aceita tipos de requisição sem diferenciar maiúsculas de minúsculas, mas sempre responde em maiúsculas.

### 5.2 Envelope comum

Toda mensagem possui exatamente esta estrutura lógica:

```json
{
  "protocol": "GDT/1.0",
  "type": "CREATE",
  "request_id": "9a77cbd27f6b4d50b4e31ff78dbc2120",
  "payload": {
    "title": "Revisar relatório",
    "description": "Conferir a seção do protocolo"
  }
}
```

| Campo | Tipo | Obrigatório | Semântica |
|---|---|---:|---|
| `protocol` | string | sim | Deve ser exatamente `GDT/1.0`. |
| `type` | string | sim | Operação, resposta ou evento. |
| `request_id` | string ou `null` | sim | Identifica uma requisição e correlaciona sua resposta. Em `EVENT` vale `null`. |
| `payload` | objeto | sim | Dados específicos da mensagem; pode ser `{}`. |

Cada requisição do cliente deve usar um `request_id` não nulo. A resposta repete o mesmo valor. Eventos espontâneos não respondem a uma requisição e, portanto, usam `null`.

### 5.3 Estados do cliente e do servidor

```text
                 conexão TCP aceita
                        │
                        ▼
                 NÃO IDENTIFICADO
                  │            │
          HELLO válido     outra mensagem
                  │            └──► ERROR/NOT_IDENTIFIED
                  ▼
                   IDENTIFICADO
       ┌──────────────┼─────────────────┐
       │              │                 │
 LIST/CREATE/...   HELLO repetido    QUIT ou falha
       │              │                 │
 resposta/evento  ERROR/ALREADY...      ▼
       └──────────────┘             ENCERRADO
```

| Estado | Eventos aceitos | Ação |
|---|---|---|
| `NÃO_IDENTIFICADO` | `HELLO` | Valida o nome, registra o cliente e envia `WELCOME`. |
| `NÃO_IDENTIFICADO` | qualquer outro tipo | Envia `ERROR` com `NOT_IDENTIFIED`; mantém o estado. |
| `IDENTIFICADO` | `LIST`, `CREATE`, `COMPLETE`, `DELETE`, `PING` | Executa, responde e permanece identificado. Alterações também geram `EVENT`. |
| `IDENTIFICADO` | novo `HELLO` | Envia `ERROR` com `ALREADY_IDENTIFIED`. |
| qualquer estado | quadro malformado, timeout ou fim do fluxo | Envia erro quando possível e encerra a conexão. |
| `IDENTIFICADO` | `QUIT` | Envia `BYE` e encerra a conexão. |

O cliente local possui estados equivalentes: `DESCONECTADO`, `CONECTANDO`, `IDENTIFICADO` e `ENCERRADO`. Ele só libera comandos após receber `WELCOME`.

### 5.4 Mensagens do cliente para o servidor

| Tipo | Estado exigido | Campos de `payload` | Resposta de sucesso |
|---|---|---|---|
| `HELLO` | não identificado | `username`: string, 1–30 caracteres após remoção de espaços externos | `WELCOME` |
| `LIST` | identificado | nenhum | `TASK_LIST` |
| `CREATE` | identificado | `title`: string de 1–100; `description`: string de 0–500 | `TASK_CREATED` |
| `COMPLETE` | identificado | `task_id`: inteiro positivo | `TASK_UPDATED` |
| `DELETE` | identificado | `task_id`: inteiro positivo | `TASK_DELETED` |
| `PING` | identificado | nenhum | `PONG` |
| `QUIT` | identificado | nenhum | `BYE`, seguida de fechamento |

### 5.5 Mensagens do servidor para o cliente

| Tipo | Conteúdo relevante de `payload` | Significado |
|---|---|---|
| `WELCOME` | `username`, `server_time`, `capabilities` | Identificação aceita. |
| `TASK_LIST` | `tasks` | Fotografia atual de todas as tarefas, ordenadas por ID. |
| `TASK_CREATED` | `task` | Confirma a criação. |
| `TASK_UPDATED` | `task` | Confirma a conclusão. |
| `TASK_DELETED` | `task` | Confirma a exclusão. |
| `PONG` | `server_time` | Confirma disponibilidade. |
| `BYE` | `message` | Confirma encerramento normal. |
| `ERROR` | `code`, `message` | Requisição recusada. |
| `EVENT` | `event`, `task`, `actor`, `occurred_at` | Mudança assíncrona no quadro. Usa `request_id: null`. |

O objeto `task` contém:

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | inteiro | Identificador monotonicamente crescente. |
| `title` | string | Título da tarefa. |
| `description` | string | Descrição opcional. |
| `completed` | booleano | Estado de conclusão. |
| `created_by` | string | Usuário que criou a tarefa. |
| `created_at` | string | Data/hora UTC ISO 8601. |
| `completed_at` | string ou `null` | Data/hora de conclusão. |

Os valores possíveis de `event` são `created`, `completed` e `deleted`.

### 5.6 Códigos de erro

| Código | Causa | Conexão permanece aberta? |
|---|---|---:|
| `BAD_MESSAGE` | UTF-8/JSON/envelope inválido ou quadro acima do limite | não |
| `TIMEOUT` | cliente inativo por mais que o limite configurado | não |
| `INVALID_REQUEST_ID` | requisição sem identificador | sim |
| `NOT_IDENTIFIED` | operação enviada antes de `HELLO` | sim |
| `INVALID_USERNAME` | nome ausente ou fora do tamanho permitido | sim |
| `ALREADY_IDENTIFIED` | segundo `HELLO` na mesma conexão | sim |
| `UNKNOWN_TYPE` | tipo de mensagem não reconhecido | sim |
| `INVALID_TITLE` | título ausente ou fora do limite | sim |
| `INVALID_DESCRIPTION` | descrição não textual ou acima do limite | sim |
| `INVALID_TASK_ID` | ID não é inteiro positivo | sim |
| `NOT_FOUND` | tarefa solicitada não existe | sim |

### 5.7 Sequência típica

```text
Cliente A                 Servidor                    Cliente B
    |--- TCP connect -------->|                           |
    |--- HELLO  req=1 ------->|                           |
    |<-- WELCOME req=1 -------|                           |
    |                         |<------ HELLO req=7 -------|
    |                         |------- WELCOME req=7 ---->|
    |--- CREATE req=2 ------->|                           |
    |<-- TASK_CREATED req=2 --|                           |
    |<-- EVENT req=null ------|------- EVENT req=null --->|
    |                         |<------ LIST req=8 --------|
    |                         |------- TASK_LIST req=8 --->|
    |--- QUIT req=3 --------->|                           |
    |<-- BYE req=3 -----------|                           |
    |--- TCP close -----------|                           |
```

Exemplo real de quadro transmitido (em uma única linha na rede):

```json
{"protocol":"GDT/1.0","type":"PING","request_id":"42","payload":{}}
```

## 6. Requisitos mínimos

### 6.1 Software

- Python 3.10 ou superior (somente biblioteca padrão);
- sistema operacional com suporte a sockets TCP: Windows, Linux ou macOS;
- terminal ou prompt de comando;
- Git apenas para clonar/publicar, não para executar.

### 6.2 Rede e recursos

- uma porta TCP disponível (padrão 5050);
- conectividade IP entre cliente e servidor quando executados em máquinas distintas;
- aproximadamente 30 MB de memória por processo, variando conforme o Python e o sistema;
- permissão de escrita no diretório de `data/tasks.json` para persistência;
- liberação da porta no firewall somente se clientes externos forem usados.

Não é necessário executar como administrador e o projeto não instala pacotes.

## 7. Como executar

### 7.1 Cenário local, em uma única máquina

Abra um terminal na raiz do projeto e inicie o servidor:

```bash
python server.py
```

Em outro terminal, inicie o primeiro cliente:

```bash
python client.py --user alice
```

Opcionalmente abra um terceiro terminal para demonstrar concorrência e eventos:

```bash
python client.py --user bob
```

Exemplo de uso no prompt `gdt>`:

```text
gdt> criar "Estudar sockets" "Ler sobre TCP e enquadramento"
gdt> listar
gdt> concluir 1
gdt> excluir 1
gdt> ping
gdt> sair
```

O título que contém espaços deve estar entre aspas. A descrição pode ser escrita depois do título.

### 7.2 Execução em máquinas diferentes

No servidor, descubra seu endereço IP e execute:

```bash
python server.py --host 0.0.0.0 --port 5050
```

No cliente, substitua o exemplo pelo IP real da máquina servidora:

```bash
python client.py --host 192.168.1.10 --port 5050 --user aluno
```

Se não houver conexão, confirme endereço, porta, firewall e se ambas as máquinas estão na mesma rede. Expor diretamente este projeto na Internet não é recomendado porque o protocolo acadêmico não implementa autenticação nem criptografia.

### 7.3 Opções disponíveis

```bash
python server.py --help
python client.py --help
```

Opções importantes do servidor:

- `--host`: interface em que o servidor escuta;
- `--port`: porta TCP;
- `--data-file`: caminho da persistência JSON;
- `--timeout`: inatividade máxima por conexão;
- `--verbose`: logs detalhados.

## 8. Testes e verificação

Todos os testes usam a biblioteca padrão `unittest`. Eles criam um servidor real em uma porta efêmera (`porta 0`) e clientes TCP reais, sem depender da porta 5050.

```bash
python -m unittest discover -v
```

Os testes cobrem:

- serialização e desserialização com caracteres Unicode;
- rejeição de versão de protocolo incorreta;
- obrigação de enviar `HELLO` antes das operações;
- criação, listagem, conclusão e exclusão;
- evento assíncrono entre dois clientes;
- validação e códigos de erro;
- persistência em disco.

Também é possível verificar a sintaxe de todos os módulos:

```bash
python -m compileall -q .
```

## 9. Estrutura do repositório

```text
.
├── client.py                 # cliente TCP e interface de terminal
├── server.py                 # servidor concorrente e armazenamento
├── protocol.py               # implementação compartilhada do GDT/1.0
├── tests/
│   ├── __init__.py
│   └── test_integration.py   # testes unitários e ponta a ponta
├── data/
│   └── .gitkeep              # mantém o diretório no Git
├── .gitignore
└── README.md                 # documentação completa
```

O arquivo `data/tasks.json` é criado automaticamente e ignorado pelo Git para não publicar dados de execução.

## 10. Decisões de implementação

- **Concorrência por threads:** é simples de observar e adequada a operações majoritariamente bloqueadas em E/S. Cada conexão possui seu próprio estado, mas compartilha o quadro protegido por bloqueio.
- **JSON:** facilita inspeção manual no Wireshark ou em logs, possui suporte nativo no Python e permite extensão futura do `payload`.
- **Delimitador LF:** resolve o problema de enquadramento do fluxo TCP com baixo custo para mensagens JSON pequenas.
- **IDs de requisição:** permitem distinguir respostas de eventos e tornam possível evoluir o cliente para várias requisições simultâneas.
- **Persistência atômica:** o arquivo temporário é completamente gravado antes da substituição do arquivo oficial.
- **Horários UTC:** evitam ambiguidade de fuso e usam ISO 8601 para interoperabilidade.
- **Estado central no servidor:** garante uma fonte única da verdade e simplifica a consistência entre clientes.

## 11. Tratamento de falhas e limitações

Quando um cliente se desconecta inesperadamente, sua thread termina e o servidor remove a conexão da lista de notificações. Uma mensagem inválida causa `ERROR`; erros que impedem reconhecer novos quadros encerram o socket para evitar dessincronização. Requisições semanticamente inválidas mantêm a conexão aberta para correção.

Limitações assumidas para o escopo acadêmico:

- não há autenticação, autorização ou TLS;
- qualquer usuário pode concluir ou excluir qualquer tarefa;
- os nomes de usuário não precisam ser exclusivos;
- o quadro é armazenado em um único servidor, sem replicação ou tolerância à queda do servidor;
- não há garantia de transação entre a confirmação enviada e o recebimento de todos os eventos;
- a persistência em JSON é apropriada para volume pequeno, não para produção em larga escala;
- o cliente de terminal exibe eventos antes do próximo prompt; a classe `TaskClient` os recebe imediatamente na fila.

Em uma versão de produção seriam recomendados TLS, autenticação, permissões, banco de dados transacional, paginação, métricas, logs estruturados e replicação.

## 12. Como publicar e entregar no GitHub

Crie um repositório **público** vazio no GitHub, sem adicionar README ou `.gitignore` pelo site. Na raiz deste projeto, execute (substituindo os campos):

```bash
git init
git add .
git commit -m "Implementa gerenciador distribuído de tarefas"
git branch -M main
git remote add origin https://github.com/SEU-USUARIO/NOME-DO-REPOSITORIO.git
git push -u origin main
```

Depois, abra a página do repositório em uma janela anônima para confirmar que ela está acessível. Copie a URL no formato abaixo e envie-a na atividade do Classroom:

```text
https://github.com/SEU-USUARIO/NOME-DO-REPOSITORIO
```

Antes da entrega, preencha os dados acadêmicos da seção 1 e evite versionar `data/tasks.json`.

## 13. Possíveis evoluções

- autenticação e controle de permissões;
- comunicação protegida por TLS;
- prazos, prioridades, responsáveis e filtros;
- armazenamento em SQLite ou PostgreSQL;
- interface gráfica ou web reutilizando a classe cliente;
- reconexão automática e recuperação de eventos;
- replicação do servidor e consenso para alta disponibilidade.

## 14. Conclusão

O projeto implementa integralmente uma aplicação distribuída cliente-servidor sobre TCP e define um protocolo de aplicação próprio. O GDT/1.0 especifica enquadramento, envelope, mensagens, eventos, estados, erros e encerramento. A implementação acrescenta concorrência, notificações bidirecionais, persistência segura e testes ponta a ponta, permitindo demonstrar na prática os conceitos apresentados na documentação.
