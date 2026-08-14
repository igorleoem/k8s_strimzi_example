# Strimzi Operator: Kafka via CRDs

The goal here is to run real Kafka on a Kubernetes cluster, entirely managed
by CRDs (`Kafka`, `KafkaNodePool`, `KafkaTopic`), and use this as a learning tool, demonstrating depth in two
subjects: Kafka + declarative IaC; connecting it to Kubernetes' native
extensibility pattern and, in particular, the operator/reconcile-loop pattern.

⚠️ **Exact Strimzi and API versions change frequently.** The manifests here use `kafka.strimzi.io/v1`
(the stable API since Strimzi 1.0.0 — earlier versions used `v1beta2`, now deprecated). If your
`kubectl get crd kafkanodepools.kafka.strimzi.io -o jsonpath='{.spec.versions[*].name}'` shows a
different version, adjust the `apiVersion` in the 3 files under `manifests/` accordingly. Always confirm
against the official quickstart before running: https://strimzi.io/quickstarts/

**Also note:** current Strimzi is **KRaft-only** — there's no ZooKeeper option anymore, so the older
`strimzi.io/kraft: enabled` and `strimzi.io/node-pools: enabled` annotations seen in older tutorials
are no longer needed (it's the only mode of operation now).

**Additional note:** testing was done on kind, but the idea is that everything works equivalently on any other Kubernetes cluster!

## 1. Install the operator

```bash
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl wait --for=condition=Ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

This installs the CRDs (`Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, etc) and brings up the
**Cluster Operator** — the controller that will watch these CRDs and act on them.
You can check which CRDs existed in the cluster before and after installing the **Operator** by running a
get on the CRDs installed in the cluster:
```bash
kubectl get crds
```

## 2. Bring up Kafka in KRaft mode (no ZooKeeper)

```bash
kubectl apply -f manifests/01-kafka-nodepool.yaml
kubectl apply -f manifests/02-kafka-cluster.yaml

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
kubectl get pods -n kafka
```

Notice what just happened: you didn't create a StatefulSet, a Service, a certificate Secret, a broker
config ConfigMap, or a PodDisruptionBudget — just a `Kafka` and a `KafkaNodePool`. The operator
translated that declarative intent into all these lower-level objects:

```bash
kubectl get all,configmap,secret -n kafka -l strimzi.io/cluster=my-cluster
```

### Persistent storage (so messages survive a pod restart)
#### **Note that** regarding persistence, this becomes a Kafka requirement, because while the server itself can be ephemeral, the messages produced have a lifespan and need to be stored for a certain period of time.

The `KafkaNodePool` here uses `storage.type: persistent-claim` (not `ephemeral`) — the operator creates
a real `PersistentVolumeClaim` for the broker, backed by kind's default `standard` StorageClass
(`local-path-provisioner`):

```bash
kubectl get pvc -n kafka
kubectl get pv
```

**Important caveat:** `local-path-provisioner` ties the volume to the **specific node**
where the pod was first scheduled — it's not portable storage like EBS on EKS. In that case, if the
broker pod gets rescheduled to a different node (e.g. after a node failure), the PVC can't reattach and
the broker gets stuck. In kind, with only 2 workers and a single-replica NodePool, this rarely shows up
in practice, but it's exactly the kind of storage-locality nuance that a managed offering like MSK
(backed by EBS, which is portable within the same AZ) hides from you.

**Quick persistence test:**
```bash
# after producing a message to a topic (see step 4 below), delete the broker pod
kubectl delete pod my-cluster-dual-role-0 -n kafka
kubectl get pods -n kafka -w   # the operator recreates it on its own

# once it's back, consume from the beginning again — the message should still be there
```
If you'd used `ephemeral` storage instead, this same test would lose all messages on pod recreation —
worth deliberately trying both to feel the difference in practice.

## 3. Create a topic via CRD

```bash
kubectl apply -f manifests/03-kafka-topic.yaml
kubectl get kafkatopic -n kafka
```

No `kafka-topics.sh --create` at all — the **Topic Operator** (part of the Entity Operator, which you
already enabled in the `Kafka` CR) translates the `KafkaTopic` into a real call to Kafka's Admin API.

## 4. Test producing/consuming

```bash
# interactive producer
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 --rm=true --restart=Never -- \
  bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic orders-events
```

A prompt will open. To produce messages, type any content and press `ENTER`. Each new line persists a new
message to the `orders-events` topic.

```bash
# in another terminal, consumer
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 --rm=true --restart=Never -- \
  bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic orders-events
```

## 5. Test the reconcile loop

This is what separates "I installed a Helm chart" from "I understand the operator pattern." Manually kill
the broker pod and watch the operator react on its own:

```bash
kubectl get pods -n kafka -l strimzi.io/name=my-cluster-dual-role
kubectl delete pod my-cluster-dual-role-0 -n kafka
kubectl get pods -n kafka -w   # the pod comes back on its own, without you asking
```

## 6. Validate the persistent volume works correctly

After killing the cluster pod, in an ephemeral scenario all messages would have been lost. But since a PV
was configured, you can verify that nothing was lost — just spin up a consumer that reads all messages
from the earliest offset.

```bash
# in the terminal, run the consumer with the "--from-beginning" flag
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.8.0 --rm=true --restart=Never -- \
  bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic orders-events --from-beginning
```

**NOTE:** At no point was any specific restart automation configured — the `Kafka` CR declares "I want
this state," and the Cluster Operator continuously compares the desired state (the CRD) against the real
state (the pods/StatefulSet), correcting any drift. This is continuous reconciliation, not a setup script
that runs once.

## 7. Cleaning up

```bash
kubectl delete -f manifests/03-kafka-topic.yaml
kubectl delete -f manifests/02-kafka-cluster.yaml
kubectl delete -f manifests/01-kafka-nodepool.yaml
kubectl delete pvc -l strimzi.io/cluster=my-cluster -n kafka
```
