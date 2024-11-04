# Lets start with python version
FROM python:3.11.10

# Set the working directory
WORKDIR /app

# Install build-essential which includes gcc and other tools
RUN apt-get update && apt-get install -y --fix-missing build-essential

# Clear the APT cache again
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install python poetry with version 1.8.3
RUN pip install poetry==1.8.3

# Copy the source code to the working directory
COPY . /app

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app

# Install the dependencies
RUN poetry install

# Run the application
CMD ["poetry", "run", "python", "src/main.py"]