# Create Kubernetes secrets for HSDS username/password  based on environment variables
if [ ${HSDS_USERNAME} ] && [ ${HSDS_PASSWORD} ]; then
  echo -n ${HSDS_USERNAME} > /tmp/hsds_username
  echo -n ${HSDS_PASSWORD} > /tmp/hsds_password

  # create the secret
  kubectl --namespace ghcn create secret generic ghcn-user --from-file=/tmp/hsds_username --from-file=/tmp/hsds_password

  # delete the temp files
  rm /tmp/hsds_username
  rm /tmp/hsds_password
fi
