FROM openjdk:8-jdk
COPY --from=python:3.7.6 / /

# Create the user that will run the app
RUN adduser --disabled-password --gecos '' ml-api-user

WORKDIR /opt/ml_api

ARG PIP_EXTRA_INDEX_URL
ENV FLASK_APP run.py

# Install requirements, including from local/Gemfury
ADD ./packages/ml_api /opt/ml_api/
#downloading local packages
ADD ./packages/regression_model /opt/regression_model/

RUN pip install --upgrade pip
RUN pip install -r /opt/ml_api/requirements.txt

RUN chmod +x /opt/ml_api/run.sh
RUN chown -R ml-api-user:ml-api-user ./

USER ml-api-user

EXPOSE 5000

CMD ["bash", "./run.sh"]