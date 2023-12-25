FROM minflow_base

# copy libs
COPY proxy.py /proxy/proxy.py
COPY startup.py /proxy/startup.py
COPY Store.py /proxy/Store.py
COPY container_config.py /proxy/container_config.py
COPY byteconcat.c /proxy/byteconcat.c
COPY setup.py /proxy/setup.py
CMD [ "python3", "/proxy/startup.py" ]
