#!/usr/bin/env bash
# ==============================================================================
# 🌐 GCP Infrastructure Setup Script for Production MLOps on Kubernetes
# ==============================================================================
# Usage:
#   chmod +x 00-gcp-setup.sh
#   ./00-gcp-setup.sh [PROJECT_ID] [ZONE] [INSTANCE_NAME]
# ==============================================================================

set -euo pipefail

PROJECT_ID="${1:-kube-mlops}"
ZONE="${2:-us-east1-b}"
INSTANCE_NAME="${3:-mlopskube-cluster}"
MACHINE_TYPE="n2-standard-8"    # 8 vCPU, 32 GB RAM (prevents CPU scheduling starvation)
BOOT_DISK_SIZE="150GB"          # 150GB SSD (prevents container layer disk saturation)

echo "=============================================================================="
echo "🚀 Provisioning GCP Compute Engine VM: ${INSTANCE_NAME}"
echo "   Project:      ${PROJECT_ID}"
echo "   Zone:         ${ZONE}"
echo "   Machine Type: ${MACHINE_TYPE} (8 vCPU, 32GB RAM)"
echo "   Boot Disk:    ${BOOT_DISK_SIZE} pd-ssd"
echo "=============================================================================="

# 1. Create Firewall Rule for MLOps Services
echo "Configuring VPC Firewall Rules..."
gcloud compute firewall-rules create allow-mlops-services \
    --project="${PROJECT_ID}" \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:80,tcp:443,tcp:3000,tcp:5500,tcp:8000,tcp:8080,tcp:8888,tcp:9000,tcp:9001,tcp:9090 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=mlops-node 2>/dev/null || echo "Firewall rule already exists."

# 2. Launch GCP VM Instance
echo "Creating Compute Engine VM..."
gcloud compute instances create "${INSTANCE_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --tags=mlops-node,http-server,https-server \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="${BOOT_DISK_SIZE}" \
    --boot-disk-type=pd-ssd \
    --metadata=startup-script='#!/bin/bash
      apt-get update -y
      apt-get install -y curl wget git jq htop ca-certificates gnupg lsb-release
      
      # Install Docker
      mkdir -p /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list
      apt-get update -y
      apt-get install -y docker-ce docker-ce-cli containerd.io
      usermod -aG docker ubuntu 2>/dev/null || true
      
      # Install Kubectl
      curl -fsSL https://dl.k8s.io/release/v1.28.3/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl
      chmod +x /usr/local/bin/kubectl
      
      # Install Helm
      curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
      
      # Install Minikube
      curl -fsSL https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 -o /usr/local/bin/minikube
      chmod +x /usr/local/bin/minikube
    ' 2>/dev/null || echo "Instance already exists or created."

echo "Waiting for instance to initialize..."
sleep 10

EXTERNAL_IP=$(gcloud compute instances describe "${INSTANCE_NAME}" --zone="${ZONE}" --format='get(networkInterfaces[0].accessConfigs[0].natIP)')
echo "=============================================================================="
echo "✅ VM Created Successfully!"
echo "   External IP:  ${EXTERNAL_IP}"
echo "   Wildcard DNS: ${EXTERNAL_IP}.nip.io"
echo "=============================================================================="
echo "To connect to your VM:"
echo "   gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE}"
echo ""
echo "Next step: Run the following on the VM to start Minikube with production resources:"
echo "   minikube start --cpus=6 --memory=24576 --disk-size=100g --driver=docker"
echo "   minikube addons enable ingress"
echo "=============================================================================="
