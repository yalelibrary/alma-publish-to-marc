FROM ubuntu:24.04

# Install dependencies
RUN apt update && apt install -y \
    python3 \
    python3-pip \
    python3.11-venv \
    git \
    libaio1 libaio-dev \
    unzip build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget

RUN mkdir /app
RUN mkdir /opt/venv
RUN python3 -m venv /opt/venv

ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /app

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

CMD ["bash"]
