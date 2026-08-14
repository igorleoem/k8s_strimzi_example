# Strimzi Operator: Kafka via CRDs

O objetivo aqui é rodar Kafka de verdade nnum cluster Kubernetes, gerenciado inteiramente
por CRDs (`Kafka`, `KafkaNodePool`, `KafkaTopic`), e usar isso como ferramenta de aprendizado , demonstrando profundidade em dois
assuntos: Kafka + IaC declarativo; conectando com o padrão nativo de
extensibilidade do Kubernetes e principalmente o padrão operator/reconcile loop.

⚠️ **As versões exatas do Strimzi e da API mudam com frequência.** Os manifests aqui usam `kafka.strimzi.io/v1`
(API estável desde o Strimzi 1.0.0 — versões anteriores usavam `v1beta2`, hoje descontinuada). Se seu
`kubectl get crd kafkanodepools.kafka.strimzi.io -o jsonpath='{.spec.versions[*].name}'` mostrar uma
versão diferente, ajuste a `apiVersion` nos 3 arquivos em `manifests/` de acordo. Confirme sempre contra
o quickstart oficial antes de rodar: https://strimzi.io/quickstarts/

**Nota também:** o Strimzi atual é **KRaft-only** — não existe mais opção de ZooKeeper, então as
anotações antigas `strimzi.io/kraft: enabled` e `strimzi.io/node-pools: enabled` que apareciam em
tutoriais mais antigos não são mais necessárias (é o único modo de operação agora).

**Nota adicional:** os testes foram realizados sobre o kind, mas a ideía é que tudo funcione de forma equivalente em qualquer outro cluster kubernetes!

## 1. Instalar o operator

```bash
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl wait --for=condition=Ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

Isso instala os CRDs (`Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, etc) e sobe o **Cluster
Operator** — o controller que vai observar esses CRDs e agir sobre eles.
Você pode verificar quais operadores exitiam no cluster antes e depois da instalçao do **Operator**, executando um get nos CRS instalados no clsuter:
```bash
kubectl get crds
```

## 2. Subir um Kafka em modo KRaft (sem ZooKeeper)

```bash
kubectl apply -f manifests/01-kafka-nodepool.yaml
kubectl apply -f manifests/02-kafka-cluster.yaml

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
kubectl get pods -n kafka
```

Repare no que aconteceu: você não criou StatefulSet, Service, Secret de certificado, ConfigMap de
config do broker, nem PodDisruptionBudget — só um `Kafka` e um `KafkaNodePool`. O operator traduziu
essa intenção declarativa em todos esses objetos de baixo nível:

```bash
kubectl get all,configmap,secret -n kafka -l strimzi.io/cluster=my-cluster
```

### Storage persistente (pra mensagens sobreviverem a um restart de pod)
#### **Note que** no ponto de persistência, isso pasa a ser um requisito do kafka, pois enquanto o servidor pode ser efêmero, as mesnagens produzidas tem um tempo de vida, necessitando estarem armazenas por um certo tempo.

O `KafkaNodePool` aqui usa `storage.type: persistent-claim` (não `ephemeral`) — o operator cria um
`PersistentVolumeClaim` de verdade pro broker, usando a StorageClass default do kind (`standard`,
via `local-path-provisioner`):

```bash
kubectl get pvc -n kafka
kubectl get pv
```

**Ressalva importante:** o `local-path-provisioner` amarra o volume ao **node específico**
onde o pod foi agendado pela primeira vez — não é storage portátil como EBS, em EKS. Nesse caso, se o pod do broker for
reagendado pra outro node (ex: depois de uma falha de node), o PVC não consegue reconectar e o broker
trava. No kind, com só 2 workers e um NodePool de 1 réplica, isso raramente aparece na prática, mas é
exatamente o tipo de nuance de localidade de storage que uma oferta gerenciada como o MSK (baseado em
EBS, que é portátil dentro da mesma AZ) esconde de você.

**Teste rápido de persistência:**
```bash
# depois de produzir uma mensagem num tópico (veja o passo 4 abaixo), apague o pod do broker
kubectl delete pod my-cluster-dual-role-0 -n kafka
kubectl get pods -n kafka -w   # o operator recria sozinho

# quando voltar, consuma do início de novo — a mensagem ainda deve estar lá
```
Se você tivesse usado storage `ephemeral`, esse mesmo teste perderia todas as mensagens na recriação do
pod — vale tentar os dois de propósito pra sentir a diferença na prática.

## 3. Criar um tópico via CRD

```bash
kubectl apply -f manifests/03-kafka-topic.yaml
kubectl get kafkatopic -n kafka
```

Sem `kafka-topics.sh --create` nenhum — o **Topic Operator** (parte do Entity Operator, que você já
habilitou no `Kafka` CR) traduz o `KafkaTopic` pra uma chamada real na Admin API do Kafka.

## 4. Testar produção/consumo

```bash
# producer interativo
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 --rm=true --restart=Never -- \
  bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic orders-events
```

Um prompt será aberto. Para produzir as mensagens, digite qualquer informação e rpesione `ENTER`. A cada nova linha, uma nova mensagem é persistida no tópico `orders-events`

```bash
# em outro terminal, consumer
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 --rm=true --restart=Never -- \
  bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic orders-events
```

## 5. Teste o reconcile loop

Isso é o que separa "eu instalei um Helm chart" de "eu entendo o padrão operator". Derrube o pod do
broker manualmente e observe o operator reagir sozinho:

```bash
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-dual-role
kubectl delete pod my-cluster-dual-role-0 -n kafka
kubectl get pods -n kafka -w   # o pod reaparece sozinho, sem você pedir
```

## 6. Valide o funcionamento correto do persistent volume

Após derrubar o pod do cluster, em um cenário efêmero, todas as mensagens teriam sido perdidas. Mas como um PV foi configurado, é possível verificar que nada foi perdido, bastante instanciar um consumer, lendo todas as mensagens, desde o offset mais antigo. 

```bash
# no terminal, rode o consumer com o parâmetro "--from-beggining"
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 --rm=true --restart=Never -- \
  bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic orders-events --from-beginning
```

** NOTA: ** Em nenhum momento foi confmigurada qualquer  automação de restart específica — o `Kafka` CR
declara "eu quero esse estado", e o Cluster Operator continuamente compara estado desejado (o CRD) com
estado real (os pods/StatefulSet), e corrige qualquer divergência. É reconciliação contínua, não um
script de setup que roda uma vez.

## 7. Limpando

```bash
kubectl delete -f manifests/03-kafka-topic.yaml
kubectl delete -f manifests/02-kafka-cluster.yaml
kubectl delete -f manifests/01-kafka-nodepool.yaml
kubectl delete pvc -l strimzi.io/cluster=my-cluster -n kafka
```