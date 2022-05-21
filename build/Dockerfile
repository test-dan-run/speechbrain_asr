# docker build -t speechbrain:v0.5.11 .

#use the base package for from official pytorch source 
FROM pytorch/pytorch:1.8.1-cuda11.1-cudnn8-runtime

ENV TZ=Asia/Singapore
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install libsndfile1 (linux soundfile package)
RUN apt-get update && apt-get install -y build-essential libsndfile1 git sox wget \
&& rm -rf /var/lib/apt/lists/*

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install pip requirements
RUN rm -rf $(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")/ruamel*
RUN python3 -m pip install --ignore-installed git+https://github.com/speechbrain/HyperPyYAML
ADD requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

#docker container starts with bash
WORKDIR /workspace
RUN ["bash"]