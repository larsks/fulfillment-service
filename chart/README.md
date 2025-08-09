# Helm chart

This directory contains the _helm_ chart used to deploy the service to an Kubernetes cluster.

The chart currently supports two variants: one for OpenShift, intended for production environments, and another
for Kind, intended for development and testing environments.

## OpenShift

Install the _cert-manager_ operator:

```shell
$ oc new-project cert-manager-operator

$ oc create -f <<.
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  namespace: cert-manager-operator
  name: cert-manager-operator
spec:
  upgradeStrategy: Default
.

$ oc create -f - <<.
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  namespace: openshift-operators
  name: cert-manager
spec:
  channel: stable
  installPlanApproval: Automatic
  name: cert-manager
  source: community-operators
  sourceNamespace: openshift-marketplace
.
```

To deploy the application run this from the root directory of the project:

```shell
$ helm install fulfillment-service chart \
--namespace innabox \
--create-namespace
--set variant=openshift
```

## Kind

To create the Kind cluster run a command like this:

```yaml
$ kind create cluster --config - <<.
apiVersion: kind.x-k8s.io/v1alpha4
kind: Cluster
name: innabox
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 30000
    hostPort: 8000
    listenAddress: 0.0.0.0
.
```

Install the _cert-manager_ operator:

```shell
$ kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
```

Deploy the application:

```shell
$ helm install fulfillment-service chart \
--namespace innabox \
--create-namespace \
--set variant=kind
```
