FROM python:3.8.5-slim-buster

LABEL maintainer="as@bb.vision"

ENV ACTIVATION_CODE Code
ENV LOCATION smart
ENV PREFERRED_PROTOCOL auto
ENV LIGHTWAY_CIPHER auto

ARG EXPRESSVPN_APP=expressvpn_3.12.0.10-1_amd64.deb

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl \
    && wget -q "https://www.expressvpn.works/clients/linux/${EXPRESSVPN_APP}" -O /tmp/${EXPRESSVPN_APP} \
    && dpkg -i /tmp/${EXPRESSVPN_APP} \ 
    && rm -rf /tmp/*.deb

RUN apt-get --allow-releaseinfo-change update \
    && apt-get install -y --no-install-recommends \
    ca-certificates figlet expect iproute2 nano sudo make gnupg procps libnm0 \
    firefox-esr xvfb zip unzip byobu bash-completion openvpn libxi6 libgconf-2-4 gnupg2

RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add - \
    && echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list \
    && sudo apt-get update \
    && sudo apt-get install -y --no-install-recommends mongodb-org \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV USERNAME bb
RUN useradd -ms /bin/bash -G sudo ${USERNAME}
USER ${USERNAME}
ARG PROFILE_FILE=/home/${USERNAME}/.bashrc

RUN echo "export PATH=/home/${USERNAME}/.local/bin:\$PATH" >> ${PROFILE_FILE} \
    && sed -i 's/#force_color_prompt/force_color_prompt/g' ${PROFILE_FILE} \
    && echo 'echo -e "\033[34;1m"; figlet -f small "Docker container"; echo -e "\033[0m"' >> ${PROFILE_FILE} \
    && echo 'PS1="\[\033[31;1m\]\u@\h:\033[34;1m\w$\033[0m "' >> ${PROFILE_FILE}

ENV DIRPATH /home/${USERNAME}/agentes
WORKDIR $DIRPATH

COPY requirements ./requirements/

RUN python -m pip install --no-cache-dir --upgrade pip setuptools \
    && python -m pip install --user --no-cache-dir -r requirements/production.txt;

USER root

COPY build/entrypoint.sh /tmp/entrypoint.sh
COPY build/expressvpn_activate.sh /tmp/expressvpn_activate.sh

ENTRYPOINT ["/bin/bash", "/tmp/entrypoint.sh"]
