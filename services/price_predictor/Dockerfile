# Dockerize the price_predictor service

# Lets start with a python version 3.11
FROM python:3.11.10

# Set the working directory to /app
WORKDIR /app

# Install build-essential which includes gcc and other tools
RUN apt-get update && apt-get install -y build-essential wget

# Dowlnoad and install ta-lib from source
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    wget -O config.guess 'http://savannah.gnu.org/cgi-bin/viewcvs/*checkout*/config/config/config.guess' && \
    wget -O config.sub 'http://savannah.gnu.org/cgi-bin/viewcvs/*checkout*/config/config/config.sub' && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib*

    

# Install python poetry with v 1.8.3
RUN pip install poetry==1.8.3

# Copy the source code to the working directory
COPY . /app

# Install the dependencies
RUN poetry install

# Run the application
CMD ["poetry", "run", "python", "src/training.py"]