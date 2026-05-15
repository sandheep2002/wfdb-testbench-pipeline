FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y gcc && dnf clean all

RUN pip install --no-cache-dir \
    numpy \
    pandas \
    wfdb \
    pymongo[srv] \
    boto3

COPY lambda_handler.py ${LAMBDA_TASK_ROOT}/lambda_handler.py

CMD ["lambda_handler.lambda_handler"]