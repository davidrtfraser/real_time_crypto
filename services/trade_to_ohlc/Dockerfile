# Lets start with python version
FROM python:3.11.10-slim

# Set the working directory
WORKDIR /app

# Install python poetry with verion 1.8.3
RUN pip install poetry==1.8.3

# Copy the source code to the working directory
COPY . /app

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app

# Install the dependencies
RUN poetry install

# Run the application
CMD ["poetry", "run", "python", "src/main.py"]
