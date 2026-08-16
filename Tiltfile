load("ext://helm_resource", "helm_resource")

docker_build("pgtask-dev", ".")

helm_resource(
    "pgtask",
    "./charts/pgtask",
    deps=["./charts/pgtask"],
    image_deps=["pgtask-dev"],
    image_keys=[("image.repository", "image.tag")],
    flags=[
        "--values=./charts/pgtask/values-development.yaml",
        "--wait",
        "--timeout=120s",
    ],
    port_forwards=["54329:5432"],
)
