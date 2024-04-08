podman build . --tag docker.io/kubernetesbigdataeg/postgresql:15.0.0-1
podman login docker.io -u kubernetesbigdataeg
podman push docker.io/kubernetesbigdataeg/postgresql:15.0.0-1