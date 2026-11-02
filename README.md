# 🧪 NoSQL & Advanced Messaging Laboratory

> **Laboratório Prático de Engenharia Backend Sênior & Especialista**  
> Dominando a tríade de persistência moderna e mensageria distribuída: **Redis 7**, **MongoDB 7**, **Apache Kafka (KRaft)** e **RabbitMQ (AMQP)**.

---

## 🏗️ Arquitetura dos Serviços (Docker Compose)

O laboratório roda localmente orquestrado via `docker-compose.yml`:

| Serviço | Imagem | Portas | Papel Arquitetural |
| :--- | :--- | :--- | :--- |
| **Redis 7** | `redis:7-alpine` | `6379` | In-Memory Key-Value, Cache-Aside e Distributed Lock atômico. |
| **MongoDB 7** | `mongo:7.0` | `27017` | NoSQL Document Store (BSON), Agregados ricos e updates atômicos ($push). |
| **Apache Kafka** | `confluentinc/cp-kafka:7.6.0` | `9092` | Event Streaming em modo KRaft (sem Zookeeper), Partições e Consumer Groups. |
| **RabbitMQ** | `rabbitmq:3.13-management-alpine` | `5672`, `15672` | AMQP Message Broker com Exchanges, Filas, DLX e Painel Web. |
| **Spring Boot App** | `Java 21 / Spring Boot 3.4` | `8083` | Microsserviço integrando todas as camadas em persistência poliglota. |

* **Painel Web do RabbitMQ:** Acesse [`http://localhost:15672`](http://localhost:15672) (Usuário: `guest` | Senha: `guest`).

---

## 📚 Módulos & Ensinamentos Práticos

---

### 🔴 Módulo 1: REDIS (Cache-Aside & Distributed Lock)

#### 1. Padrão Cache-Aside (Lazy Loading) com TTL
* **Mecanismo:** A aplicação consulta primeiro o Redis (`product:{id}`).
  - **Cache Hit (< 3ms):** Dado retornado da memória RAM. Carga zero no banco relacional.
  - **Cache Miss (~120ms):** Dado lido do banco, salvo no Redis com TTL de 45 segundos e retornado ao cliente.
* **Invalidação de Cache (Cache Eviction):** Ao atualizar o preço de um produto, o Redis **apaga a chave** (`redisTemplate.delete`). O próximo GET recarrega o dado fresco de forma consistente.

#### 2. Distributed Lock com Redis (`SET NX PX` + Script Lua)
* **Por que `synchronized` e `ReentrantLock` falham em nuvem?** Eles residem na memória Heap de uma única JVM. Em ambientes com múltiplos Pods/instâncias, réplicas concorrentes executam a mesma seção crítica simultaneamente, gerando race conditions.
* **A Solução Distribuída:**
  - `SET lock:key token NX PX 5000`: Operação atômica no Redis. O `NX` impede a criação duplicada e o `PX` (TTL de 5s) evita deadlocks se a instância sofrer crash/OOMKilled.
  - **Liberação Segura com Script Lua:** Impede que um processo demorado apague por engano o lock adquirido por outro pod:
    ```lua
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    ```

---

### 🍃 Módulo 2: MONGODB (Document Store & Persistência Poliglota)

#### 1. SQL Normalizado vs. MongoDB Agregado
* **SQL:** Normalização em várias tabelas (`pedidos`, `itens`, `enderecos`, `historico`), exigindo múltiplos `JOINs` custosos para recompor o objeto.
* **MongoDB:** Agregado salvo como **1 único documento BSON**. Zero JOINs na leitura.

#### 2. Embedding vs. Referencing (Decisão de Arquitetura)
* **Embedding (Embutir):** Para relações 1:1 ou 1:Poucos (itens do pedido, endereço, histórico de status). Garante atomicidade em uma única escrita.
* **Referencing (Referenciar):** Para relações 1:Muitos infinitas ou dados compartilhados, respeitando o **limite rígido de 16 MB por documento** do MongoDB.

#### 3. Atualizações Atômicas de Baixo Nível (`$push` e `$set`)
* **Antipattern:** Ler o documento inteiro de 200 KB na memória do Java, alterar a lista e salvar o documento inteiro de volta.
* **Padrão Sênior:** Usar `MongoTemplate.updateFirst()` com operadores nativos `$push` (para anexar ao array) e `$set` (para atualizar status), executando a alteração atômica direto no disco do MongoDB em poucos milissegundos.

#### 4. Trade-offs do NoSQL: O que considerar antes de adotar?
* Falta de integridade referencial estrita (sem Foreign Keys no banco; responsabilidade transferida para o código Java).
* Duplicação de dados e custo de atualização em massa (*Update Anomalies*).
* Consultas analíticas e relatórios complexos com `$lookup` são custosos em CPU e memória RAM se comparados ao SQL tradicional.

---

### 📬 Módulo 3: APACHE KAFKA (Event Streaming & Log Distribuído)

#### 1. Log Distribuído vs. Fila Tradicional
* O Kafka **não apaga mensagens após o consumo**! Elas são gravadas sequencialmente em disco e retidas por tempo (7 dias, 30 dias ou indefinido).
* O consumidor apenas move um marcador numérico (**Offset**). Permite **Event Sourcing** e reprocessar eventos do passado (*Replay*).

#### 2. Garantia de Ordem & Hashing de Chaves
* O Kafka garante ordem cronológica estrita **APENAS DENTRO DA MESMA PARTIÇÃO**.
* Ao enviar eventos com **Chave (`key = orderId`)**, o Kafka aplica `hash(key) % num_partitions`, garantindo que todos os acontecimentos daquele pedido caiam na mesma partição e sejam processados em ordem FIFO perfeita.
* **Armadilha:** Aumentar o número de partições em um tópico ativo altera o cálculo `hash % N`, podendo quebrar a ordem cronológica de mensagens futuras de uma mesma chave!

#### 3. Consumer Groups: A Dupla Dimensão
* **Entre Grupos Diferentes (Fan-Out / Pub-Sub):** Cada Consumer Group (`audit-group`, `notification-group`) recebe **100% de todas as mensagens** do tópico no seu próprio ritmo, sem duplicar arquivos de fila.
* **Dentro do Mesmo Grupo (Load Balancing):** Os pods da mesma aplicação dividem as partições entre si para escala horizontal.
* **Regra Matemática:** O número máximo de consumidores ativos em um grupo é igual ao número de partições do tópico. Instâncias excedentes ficam ociosas.

#### 4. Observabilidade & Consumer Lag
* $\text{Consumer Lag} = \text{Último Offset Produzido} - \text{Último Offset Consumido}$.
* Monitoramento via Prometheus + Grafana usando **Kafka Exporter** no cluster para garantir visibilidade mesmo se a aplicação consumidora sofrer crash.

---

### 🐇 Módulo 4: RABBITMQ (AMQP, Exchanges, Filas, DLX & Manual ACK/NACK)

#### 1. O Modelo AMQP: Produtor -> Exchange -> Fila -> Consumidor
No RabbitMQ, o produtor **nunca envia direto para a fila**. Ele envia para uma **Exchange**, que roteia a mensagem para as filas com base em regras (**Bindings**) e chaves de roteamento (**Routing Keys**).

#### 2. Tipos de Exchange
* **Direct Exchange (`orders.direct.exchange`):** Roteamento exato ponto-a-ponto (`order.payment.process`).
* **Topic Exchange (`orders.topic.exchange`):** Roteamento flexível com curingas:
  - `*` (asterisco): Substitui exatamente **uma** palavra (ex: `orders.br.*` captura `orders.br.pix` e `orders.br.cartao`).
  - `#` (cerquilha): Substitui **zero ou mais** palavras (ex: `orders.#` captura qualquer mensagem iniciada por `orders.`).

#### 3. Confiabilidade com Manual ACK e NACK
* **Auto-ACK (`none`):** O broker apaga a mensagem assim que ela viaja pelo cabo de rede. Se a aplicação morrer no meio do processamento, **o dado é perdido para sempre**!
* **Manual ACK (`basicAck`):** O RabbitMQ mantém a mensagem salva até a aplicação confirmar o sucesso.
* **Manual NACK (`basicNack` com `requeue=false`):** Em caso de falha de negócio, a aplicação rejeita a mensagem sem reenfileirar (evitando o loop infinito da "Pílula de Veneno" - *Poison Pill*). O RabbitMQ despacha a mensagem automaticamente para a **Dead-Letter Exchange (DLX)**!

---

## ⚖️ Tabela Definitiva: Kafka vs. RabbitMQ vs. SQS

| Requisito do Projeto | Use KAFKA | Use RABBITMQ | Use AWS SQS |
| :--- | :---: | :---: | :---: |
| **Volume de milhões de eventos/segundo** | 🏆 **Sim (Zero-Copy Log)** | Não | Fica caro em altíssimo volume |
| **Reprocessar eventos do passado (Replay)** | 🏆 **Sim (Offsets)** | Não (Apaga no ACK) | Não (Apaga no ACK) |
| **Roteamento complexo com regras e curingas** | Básico | 🏆 **Sim (Exchanges Topic/Headers)** | Básico |
| **Fila de Prioridade (Mensagem fura-fila)** | Não | 🏆 **Sim** | Não |
| **Zero complexidade de infraestrutura/servidor** | Não (Operação pesada) | Não (Operação de cluster) | 🏆 **Sim (100% Serverless)** |
| **Fan-Out sem duplicar filas físicas** | 🏆 **Sim (Consumer Groups)** | Não (Cria 1 fila por sistema) | Não (Cria 1 fila SQS por sistema) |

---

## 🧪 Guia Completo de Testes Práticos (cURL)

### 1. Testes do Redis (Porta 8083)
```powershell
# Cache-Aside (1ª chamada MISS ~120ms, 2ª chamada HIT < 3ms):
curl.exe -s "http://localhost:8083/api/redis/products/1"
curl.exe -s "http://localhost:8083/api/redis/products/1"

# Invalidação de Cache após alteração de preço:
curl.exe -s -X PUT "http://localhost:8083/api/redis/products/1/price?newPrice=2999.00"

# Compra Concorrente com Distributed Lock (SET NX PX):
curl.exe -s -X POST "http://localhost:8083/api/redis/flash-sale/buy?productId=1&customerId=Leo"
```

### 2. Testes do MongoDB (Porta 8083)
```powershell
# Criar documento completo com Embedding de Endereço e Timeline:
curl.exe -s -X POST "http://localhost:8083/api/mongodb/orders?orderId=PED-2001&customerId=CLI-LEO&amount=4999.00&street=Av+Paulista&city=Sao+Paulo"

# Atualização Atômica via $push (sem ler o documento no Java):
curl.exe -s -X POST "http://localhost:8083/api/mongodb/orders/PED-2001/events?status=PAID&description=Pagamento+aprovado"

# Leitura rápida do Agregado Completo (Zero JOINs):
curl.exe -s "http://localhost:8083/api/mongodb/orders/PED-2001"
```

### 3. Testes do Apache Kafka (Porta 8083)
```powershell
# Publicar 2 eventos do mesmo pedido (mesma chave -> mesma partição sequencial):
curl.exe -s -X POST "http://localhost:8083/api/kafka/publish?orderId=PED-9001&amount=1500.00&eventType=ORDER_CREATED"
curl.exe -s -X POST "http://localhost:8083/api/kafka/publish?orderId=PED-9001&amount=1500.00&eventType=PAYMENT_CONFIRMED"

# Publicar evento de outro pedido (outra partição):
curl.exe -s -X POST "http://localhost:8083/api/kafka/publish?orderId=PED-9002&amount=250.00&eventType=ORDER_CREATED"

# Verificar o consumo independente dos DOIS Consumer Groups (Fan-Out):
curl.exe -s "http://localhost:8083/api/kafka/consumed-history"
```

### 4. Testes do RabbitMQ (Porta 8083)
```powershell
# Teste 1: Sucesso com MANUAL ACK (processado e removido da fila principal):
curl.exe -s -X POST "http://localhost:8083/api/rabbitmq/payment?orderId=PED-7001&amount=450.00&simulateError=false"

# Teste 2: Falha com MANUAL NACK -> Enviado automaticamente para a DLQ:
curl.exe -s -X POST "http://localhost:8083/api/rabbitmq/payment?orderId=PED-7002&amount=999.00&simulateError=true"

# Consultar o histórico de mensagens salvas na Dead-Letter Queue (DLQ):
curl.exe -s "http://localhost:8083/api/rabbitmq/dlq-history"

# Teste 3: Topic Exchange com Curingas (* e #):
# Cai na Fila Brasil (orders.br.*) E na Fila Global (orders.#):
curl.exe -s -X POST "http://localhost:8083/api/rabbitmq/topic?routingKey=orders.br.pix&content=PixRecebidoNoBrasil"

# Cai APENAS na Fila Global (orders.#):
curl.exe -s -X POST "http://localhost:8083/api/rabbitmq/topic?routingKey=orders.us.card&content=CartaoRecebidoNosEUA"

# Consultar quais filas receberam as mensagens do Topic:
curl.exe -s "http://localhost:8083/api/rabbitmq/topic-history"
```
