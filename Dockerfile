# Pull Python base image
FROM python:3.11

# install the toolbox runner tools
RUN pip install "json2args>=0.6.2"

# install stgrid2area
RUN pip install stgrid2area

# create the tool input structure
RUN mkdir /in
#COPY ./in /in
RUN mkdir /out
RUN mkdir /src
COPY ./src /src

# additional folder where input data (stgrid and areas) is mounted
RUN mkdir /data

# copy the citation file
COPY ./CITATION.cf[f] /src/CITATION.cff

# open Dask dashboard port, use flag -p 8787:8787 when running the container
EXPOSE 8787

WORKDIR /src
CMD ["python", "run.py"]
