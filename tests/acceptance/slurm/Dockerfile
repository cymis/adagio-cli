FROM ghcr.io/cymis/qiime2-plugin-feature-table:2026.7
USER root
ENV PATH=/opt/conda/envs/qiime2-tiny-2026.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN /usr/bin/apt-get update && DEBIAN_FRONTEND=noninteractive /usr/bin/apt-get install -y --no-install-recommends slurm-wlm slurmdbd mariadb-server munge wget ca-certificates && /usr/bin/apt-get clean
RUN wget -q https://github.com/apptainer/apptainer/releases/download/v1.5.3/apptainer_1.5.3-trixie+_amd64.deb -O /tmp/apptainer.deb && /usr/bin/apt-get install -y /tmp/apptainer.deb
# Python 3.12 launcher avoids the base Conda Python 3.14 emulation fault on Apple Silicon.
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-py312_26.5.3-2-Linux-x86_64.sh -O /tmp/conda.sh && bash /tmp/conda.sh -b -p /opt/test-conda && rm /tmp/conda.sh
RUN /opt/conda/envs/qiime2-tiny-2026.7/bin/pip install cyclopts pydantic rich tomli
ENTRYPOINT ["/bin/bash"]
