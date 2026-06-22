#!/bin/sh
echo "Submitting GPU Job to Slurm Queue for Customer: $CUSTOMER_ID..."

if [ "${MOCK_GPU:-false}" = "true" ]; then
  echo "MOCK_GPU=true; validated allocation request for $CUSTOMER_ID with $GPU_COUNT requested GPU(s)."
  exit 0
fi

# Generate the dynamic Slurm batch script using environment variables
cat << EOF > actual_payload.sh
#!/bin/bash
#SBATCH --job-name=tenant-$CUSTOMER_ID
#SBATCH --gres=gpu:$GPU_COUNT
#SBATCH --time=00:10:00

nvidia-smi
EOF

# Execute the submission to Slurm
sbatch --cluster=k8s-slurm actual_payload.sh
