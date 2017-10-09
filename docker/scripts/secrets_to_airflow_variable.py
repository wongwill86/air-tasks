import os
from airflow.models import Variable

if os.path.isdir('/run/secrets/'):
    for key in os.listdir:
        with open(key, 'r') as f:
            value = f.read()
        Variable.set('%s.secret' % key, value)
        print('Setting key %s as airflow variable: %s.secret' % (key, key))
