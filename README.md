# Gerenciador Distribuído de Tarefas (GDT)

Trabalho desenvolvido para a disciplina de Redes de Computadores. A proposta é implementar uma aplicação distribuída no modelo cliente-servidor e definir um protocolo próprio da camada de aplicação.

**Aluno(a):** preencher antes da entrega  
**Disciplina/turma:** preencher antes da entrega  
**Professor(a):** preencher antes da entrega  
**Data:** preencher antes da entrega

## 1. Sobre o projeto

Para este trabalho, escolhi desenvolver um gerenciador de tarefas compartilhado. A ideia é simples: várias pessoas podem abrir o cliente em computadores diferentes e acessar o mesmo quadro de tarefas. É possível criar uma tarefa, consultar a lista, marcar uma tarefa como concluída e excluí-la.

Escolhi esse tipo de aplicação porque ele permite observar com clareza o papel de cada parte. O servidor mantém o estado do quadro e os clientes enviam comandos. Quando um cliente altera alguma tarefa, os outros clientes conectados recebem uma notificação. Dessa forma, o programa não fica limitado a uma única troca de pergunta e resposta.

Principais características:

- comunicação por sockets TCP;
- vários clientes atendidos ao mesmo tempo;
- mensagens no formato JSON;
- notificações de alteração enviadas pelo servidor;
- tarefas salvas em `data/tasks.json`;
- tratamento de mensagens inválidas e erros de operação;
- testes automatizados usando apenas a biblioteca padrão do Python.

## 2. Organização e funcionamento da aplicação

A aplicação possui três módulos principais:

- `server.py`: recebe as conexões, processa os comandos e mantém as tarefas;
- `client.py`: apresenta o terminal interativo usado pelo usuário;
- `protocol.py`: define o formato das mensagens e faz a codificação e decodificação do protocolo.

```text
 Cliente A ─────┐
                │     conexão TCP       ┌─────────────────┐
 Cliente B ─────┼──────────────────────►│    Servidor     │
                │◄──────────────────────│   server.py     │
 Cliente C ─────┘                       └────────┬────────┘
                                                │
                                                ▼
                                         data/tasks.json
```

Ao ser iniciado, o servidor abre a porta TCP 5050 por padrão e fica aguardando conexões. Para cada cliente é criada uma thread de atendimento. Isso permite, por exemplo, que um cliente liste as tarefas enquanto outro está criando uma nova.

O primeiro comando de um cliente deve ser `HELLO`, que informa o nome do usuário. Depois dessa identificação, o servidor aceita os comandos normais da aplicação. As alterações no quadro são protegidas por um bloqueio (`RLock`) para evitar que duas threads modifiquem os dados ao mesmo tempo.

As tarefas ficam em memória enquanto o servidor está funcionando e também são gravadas em `data/tasks.json`. A gravação usa primeiro um arquivo temporário e depois substitui o arquivo anterior. Isso diminui a chance de deixar um JSON incompleto se ocorrer uma falha durante a escrita.

No lado do cliente existe uma thread dedicada a receber mensagens. Ela separa as respostas normais das notificações assíncronas enviadas pelo servidor. A associação entre uma requisição e sua resposta é feita pelo campo `request_id`.

## 3. Por que foi usado TCP

Usei TCP porque as mensagens modificam um estado compartilhado. Nesse caso, perder uma solicitação de criação ou receber comandos fora de ordem poderia deixar o quadro diferente do esperado.

O TCP entrega os bytes em ordem e já possui mecanismos de confirmação, retransmissão, controle de fluxo e detecção do encerramento da conexão. A conexão persistente também é útil porque o servidor pode enviar uma notificação ao cliente assim que outra pessoa altera uma tarefa.

UDP poderia ser usado, mas seria necessário implementar na própria aplicação confirmações, retransmissões, ordenação e controle de mensagens duplicadas. Para este projeto, isso aumentaria a complexidade sem trazer uma vantagem prática.

É importante lembrar que o TCP entrega um fluxo de bytes, e não mensagens prontas. Por isso, o protocolo da aplicação usa uma quebra de linha (`\n`) para indicar onde cada mensagem termina.

## 4. Requisitos mínimos

- Python 3.10 ou mais recente;
- Windows, Linux ou macOS com suporte a sockets TCP;
- uma porta TCP disponível (5050 por padrão);
- permissão para gravar no diretório do projeto;
- conexão de rede entre as máquinas, caso cliente e servidor não estejam no mesmo computador.

Não é preciso instalar bibliotecas externas. Todo o projeto usa módulos que já acompanham o Python.

## 5. Como executar

### Teste no mesmo computador

Abra um terminal na pasta do projeto e execute o servidor:

```bash
python server.py
```

Em outro terminal, abra um cliente:

```bash
python client.py --user alice
```

Para testar a comunicação entre vários clientes, abra mais um terminal:

```bash
python client.py --user bob
```

Comandos disponíveis no cliente:

```text
listar
criar "Título da tarefa" descrição opcional
concluir ID
excluir ID
ping
ajuda
sair
```

Um exemplo de uso seria:

```text
gdt> criar "Revisar o trabalho" "Conferir a descrição do protocolo"
Tarefa criada:
[ ] #1 Revisar o trabalho — Conferir a descrição do protocolo (por alice)

gdt> listar
[ ] #1 Revisar o trabalho — Conferir a descrição do protocolo (por alice)

gdt> concluir 1
[✓] #1 Revisar o trabalho — Conferir a descrição do protocolo (por alice)
```

### Teste em computadores diferentes

Na máquina que será o servidor:

```bash
python server.py --host 0.0.0.0 --port 5050
```

Na máquina cliente, informe o endereço IP do servidor:

```bash
python client.py --host 192.168.1.10 --port 5050 --user alice
```

O endereço `192.168.1.10` é apenas um exemplo e deve ser trocado pelo IP correto. Se a conexão não funcionar, é necessário conferir o firewall, a porta e se as máquinas conseguem se comunicar na rede.

As demais opções podem ser consultadas com:

```bash
python server.py --help
python client.py --help
```

## 6. Protocolo da camada de aplicação

O protocolo criado para o trabalho foi chamado de **GDT/1.0**. O nome serve para identificar a aplicação e sua versão.

### 6.1 Formato e enquadramento

Cada mensagem é um objeto JSON codificado em UTF-8. Na rede, o objeto é enviado em uma única linha e termina com o caractere `LF` (`\n`, valor hexadecimal `0x0A`). O limite de uma mensagem, incluindo esse delimitador, é 65.536 bytes.

Exemplo de mensagem exatamente como ela é enviada no socket:

```json
{"protocol":"GDT/1.0","type":"PING","request_id":"42","payload":{}}
```

Todas as mensagens possuem quatro campos:

| Campo | Tipo | Função |
|---|---|---|
| `protocol` | string | Identifica a versão. Deve conter `GDT/1.0`. |
| `type` | string | Informa o comando, a resposta ou o evento. |
| `request_id` | string ou `null` | Relaciona uma resposta com a requisição que a originou. |
| `payload` | objeto JSON | Guarda os dados específicos daquela mensagem. |

Exemplo de uma solicitação para criar uma tarefa:

```json
{
  "protocol": "GDT/1.0",
  "type": "CREATE",
  "request_id": "b3167e15db184617a7664d275e5e9e92",
  "payload": {
    "title": "Preparar apresentação",
    "description": "Separar uma demonstração do cliente"
  }
}
```

O `request_id` é criado pelo cliente e repetido pelo servidor na resposta. Assim, o cliente sabe a qual comando aquela resposta pertence. A única exceção é a mensagem `EVENT`, que parte do servidor sem ter sido solicitada e usa `request_id` igual a `null`.

### 6.2 Estados de uma conexão

O servidor considera três estados para cada cliente:

```text
              conexão TCP aceita
                      │
                      ▼
              NÃO IDENTIFICADO
                      │
                 HELLO válido
                      │
                      ▼
                 IDENTIFICADO
                      │
          QUIT, erro fatal ou desconexão
                      │
                      ▼
                   ENCERRADO
```

#### Estado `NÃO IDENTIFICADO`

É o estado inicial. O servidor espera uma mensagem `HELLO`. Se receber qualquer outro comando, responde com o erro `NOT_IDENTIFIED` e continua aguardando a identificação.

#### Estado `IDENTIFICADO`

Começa depois que o servidor aceita o `HELLO` e envia `WELCOME`. Nesse estado, o cliente pode usar `LIST`, `CREATE`, `COMPLETE`, `DELETE`, `PING` e `QUIT`. Um novo `HELLO` não é aceito na mesma conexão.

#### Estado `ENCERRADO`

Ocorre depois de `QUIT`, do fim da conexão TCP, de inatividade acima do limite ou de um erro que impeça a leitura correta das próximas mensagens.

No programa cliente, o fluxo correspondente é `DESCONECTADO` → `CONECTANDO` → `IDENTIFICADO` → `ENCERRADO`. Os comandos só são liberados após o recebimento de `WELCOME`.

### 6.3 Eventos tratados

Os principais eventos que fazem a conexão mudar de estado ou executar alguma ação são:

| Evento | Estado atual | Resultado |
|---|---|---|
| conexão TCP aceita | — | servidor cria o atendimento no estado `NÃO IDENTIFICADO` |
| `HELLO` válido | não identificado | registra o usuário, envia `WELCOME` e muda para `IDENTIFICADO` |
| comando antes de `HELLO` | não identificado | envia `ERROR/NOT_IDENTIFIED` |
| comando de consulta | identificado | envia a resposta com o mesmo `request_id` |
| comando de alteração | identificado | altera o quadro, responde e envia `EVENT` aos clientes |
| segundo `HELLO` | identificado | envia `ERROR/ALREADY_IDENTIFIED` |
| JSON ou quadro inválido | qualquer estado | envia `BAD_MESSAGE`, quando possível, e fecha a conexão |
| tempo de inatividade esgotado | qualquer estado | envia `TIMEOUT` e fecha a conexão |
| `QUIT` | identificado | envia `BYE` e fecha a conexão normalmente |
| desconexão inesperada | qualquer estado | remove o cliente da lista de conexões ativas |

### 6.4 Mensagens enviadas pelo cliente

| Tipo | Conteúdo do `payload` | Resposta esperada |
|---|---|---|
| `HELLO` | `username`: texto de 1 a 30 caracteres | `WELCOME` |
| `LIST` | objeto vazio | `TASK_LIST` |
| `CREATE` | `title`: 1 a 100 caracteres; `description`: até 500 | `TASK_CREATED` |
| `COMPLETE` | `task_id`: inteiro positivo | `TASK_UPDATED` |
| `DELETE` | `task_id`: inteiro positivo | `TASK_DELETED` |
| `PING` | objeto vazio | `PONG` |
| `QUIT` | objeto vazio | `BYE` e encerramento da conexão |

### 6.5 Mensagens enviadas pelo servidor

| Tipo | Conteúdo do `payload` | Quando é enviada |
|---|---|---|
| `WELCOME` | `username`, `server_time`, `capabilities` | após aceitar a identificação |
| `TASK_LIST` | `tasks` | em resposta a `LIST` |
| `TASK_CREATED` | `task` | em resposta a `CREATE` |
| `TASK_UPDATED` | `task` | em resposta a `COMPLETE` |
| `TASK_DELETED` | `task` | em resposta a `DELETE` |
| `PONG` | `server_time` | em resposta a `PING` |
| `BYE` | `message` | em resposta a `QUIT` |
| `ERROR` | `code`, `message` | quando uma mensagem ou operação é recusada |
| `EVENT` | `event`, `task`, `actor`, `occurred_at` | quando uma tarefa é alterada |

Uma tarefa enviada pelo protocolo possui os campos abaixo:

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | inteiro | identificador da tarefa |
| `title` | string | título |
| `description` | string | descrição, que pode estar vazia |
| `completed` | booleano | indica se a tarefa foi concluída |
| `created_by` | string | usuário que criou a tarefa |
| `created_at` | string | data e hora de criação em UTC e ISO 8601 |
| `completed_at` | string ou `null` | data da conclusão, quando houver |

### 6.6 Notificações (`EVENT`)

Depois de criar, concluir ou excluir uma tarefa, o servidor envia um `EVENT` para todos os clientes identificados. Os valores do campo `event` são:

- `created`: uma tarefa foi criada;
- `completed`: uma tarefa foi concluída;
- `deleted`: uma tarefa foi excluída.

Exemplo:

```json
{
  "protocol": "GDT/1.0",
  "type": "EVENT",
  "request_id": null,
  "payload": {
    "event": "completed",
    "task": {
      "id": 1,
      "title": "Preparar apresentação",
      "description": "",
      "completed": true,
      "created_by": "alice",
      "created_at": "2026-11-17T14:20:00+00:00",
      "completed_at": "2026-11-17T14:25:00+00:00"
    },
    "actor": "bob",
    "occurred_at": "2026-11-17T14:25:00+00:00"
  }
}
```

### 6.7 Erros do protocolo

Todos os erros usam o tipo `ERROR`. O `payload` possui um código para o programa e uma mensagem que pode ser mostrada ao usuário.

| Código | Motivo |
|---|---|
| `BAD_MESSAGE` | JSON, codificação ou envelope inválido; também usado quando a mensagem passa do limite |
| `TIMEOUT` | conexão inativa por tempo demais |
| `INVALID_REQUEST_ID` | requisição enviada sem identificador |
| `NOT_IDENTIFIED` | comando enviado antes de `HELLO` |
| `INVALID_USERNAME` | nome de usuário ausente ou fora do tamanho permitido |
| `ALREADY_IDENTIFIED` | novo `HELLO` depois da identificação |
| `UNKNOWN_TYPE` | tipo de mensagem desconhecido |
| `INVALID_TITLE` | título vazio ou maior que 100 caracteres |
| `INVALID_DESCRIPTION` | descrição maior que 500 caracteres ou com tipo incorreto |
| `INVALID_TASK_ID` | identificador que não é um inteiro positivo |
| `NOT_FOUND` | tarefa inexistente |

Erros relacionados ao conteúdo de um comando não fecham a conexão. Já `BAD_MESSAGE` e `TIMEOUT` encerram o atendimento, pois não é seguro continuar lendo o fluxo ou manter uma conexão abandonada.

### 6.8 Exemplo de troca completa

```text
Cliente A                    Servidor                    Cliente B
    |---- conexão TCP ---------->|                           |
    |---- HELLO (id 1) --------->|                           |
    |<--- WELCOME (id 1) --------|                           |
    |                            |<----- HELLO (id 8) -------|
    |                            |------ WELCOME (id 8) ---->|
    |---- CREATE (id 2) -------->|                           |
    |<--- TASK_CREATED (id 2) ---|                           |
    |<--- EVENT (id null) -------|------ EVENT (id null) --->|
    |                            |<----- LIST (id 9) --------|
    |                            |------ TASK_LIST (id 9) --->|
    |---- QUIT (id 3) ---------->|                           |
    |<--- BYE (id 3) ------------|                           |
    |       conexão fechada      |                           |
```

## 7. Testes

Os testes podem ser executados com:

```bash
python -m unittest discover -v
```

Eles iniciam um servidor em uma porta livre e usam conexões TCP reais. São verificados:

- codificação e decodificação das mensagens;
- rejeição de uma versão incorreta do protocolo;
- obrigatoriedade do `HELLO` como primeira mensagem;
- criação, listagem, conclusão e exclusão de tarefas;
- envio de evento entre dois clientes;
- códigos de erro;
- gravação das tarefas em disco.

Na última verificação, os seis testes foram executados com sucesso.

## 8. Estrutura dos arquivos

```text
.
├── client.py
├── server.py
├── protocol.py
├── data/
│   └── .gitkeep
├── tests/
│   ├── __init__.py
│   └── test_integration.py
├── .gitignore
└── README.md
```

O arquivo `data/tasks.json` aparece somente depois que uma tarefa é criada. Ele está no `.gitignore` para que os dados de cada execução não sejam enviados ao repositório.

## 9. Limitações conhecidas

O programa foi pensado para uma demonstração acadêmica em rede local. Ele não possui autenticação, criptografia TLS nem níveis de permissão. Os nomes de usuário também não precisam ser únicos e qualquer cliente pode concluir ou excluir uma tarefa.

Além disso, existe apenas um servidor. Se ele for encerrado, os clientes perdem a conexão, embora as tarefas já gravadas possam ser carregadas novamente na próxima execução. O arquivo JSON é suficiente para o volume deste trabalho, mas um sistema com muitos usuários deveria usar um banco de dados.

Essas limitações não impedem o objetivo da atividade, que é demonstrar a comunicação cliente-servidor, o uso de sockets e a definição de um protocolo de aplicação.
