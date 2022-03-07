FROM ubuntu:20.04

# Avoid freezing during apt install while waiting for input.
ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt update

RUN apt install -q -y curl
RUN curl -s -L -o /bin/joppin https://github.com/madogiwa/joppin/releases/latest/download/joppin_linux_amd64
RUN chmod +x /bin/joppin

RUN apt install -q -y awscli netcat
