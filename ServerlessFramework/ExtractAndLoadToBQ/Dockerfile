FROM public.ecr.aws/lambda/python:3.9 

COPY lambda_function.py ./lambda_function.py
COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

CMD ["lambda_function.lambda_handler"]