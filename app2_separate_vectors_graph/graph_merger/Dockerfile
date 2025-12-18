# Use the AWS Lambda Python 3.11 base image
# You can switch to python:3.9 or python:3.10 if preferred
FROM public.ecr.aws/lambda/python:3.11

# Copy the requirements file to the container
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code to the container
COPY app.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could be app.lambda_handler)
CMD [ "app.lambda_handler" ]