

gcloud beta compute instance-groups managed update pubsub-mig-prod \
    --project=labs-169405 \
    --zone=asia-northeast1-a \
    --update-policy-type=opportunistic \
    --default-action-on-vm-failure=repair \
    --action-on-vm-failed-health-check=default-action \
    --no-force-update-on-repair \
    --standby-policy-mode=manual \
    --suspended-size=0 \
    --stopped-size=0 \
    --standby-policy-initial-delay=0 \
    --list-managed-instances-results=pageless && \
gcloud beta compute instance-groups managed rolling-action start-update pubsub-mig-prod \
    --project=labs-169405 \
    --zone=asia-northeast1-a \
    --type=opportunistic \
    --version=template=projects/labs-169405/regions/asia-northeast1/instanceTemplates/instance-template-pubsub-mig-prod && \
gcloud beta compute instance-groups managed set-autoscaling pubsub-mig-prod \
    --project=labs-169405 \
    --zone=asia-northeast1-a \
    --mode=on \
    --min-num-replicas=2 \
    --max-num-replicas=3 \
    --target-cpu-utilization=0.9 \
    --cpu-utilization-predictive-method=none \
    --cool-down-period=60 && \
gcloud compute instance-groups set-named-ports pubsub-mig-prod \
    --project=labs-169405 \
    --zone=asia-northeast1-a \
    --named-ports=http:50051