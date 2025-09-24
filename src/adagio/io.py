from parsl import python_app

from adagio.execution.proxy import ProxyMetadata, lift_parsl, ProxyArtifact


@lift_parsl(lambda fut: ProxyArtifact(fut, 'artifact'))
def load_input(*, ctx, source: str):
    from qiime2.sdk import Results, Artifact
    from qiime2.sdk import PluginManager
    PluginManager()

    with ctx.cache:
        input = Artifact.load(source)

    return Results(['artifact'], [input])

@lift_parsl(ProxyMetadata)
def load_metadata(*, ctx, source: str):
    from qiime2 import Artifact, Metadata
    import zipfile
    if zipfile.is_zipfile(source):
        metadata = Artifact.load(source).view(Metadata)
    else:
        metadata = Metadata.load(source)

    return metadata



@lift_parsl(lambda fut: fut)
def save_output(*, ctx, output, destination):
    output.save(destination)


@lift_parsl(ProxyMetadata)
def convert_metadata(*, ctx, metadata):
    import qiime2

    if isinstance(metadata, qiime2.Artifact):
        metadata = metadata.view(qiime2.Metadata)

    return metadata