# Use AWS Lambda Python 3.12 base image
FROM public.ecr.aws/lambda/python:3.12

# Copy requirements file
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
# Copy any other modules/folders if you have them
# COPY utils/ ${LAMBDA_TASK_ROOT}/utils/

# Set the CMD to your handler (could also be done in SAM template)
CMD [ "app.lambda_handler" ]