import subprocess
from flask import Flask
from flask import request

app = Flask(__name__)

global_job_id = 100


@app.route('/spot_launch/', methods=['POST'])
def spot_launch():
    job_id = request.values.get('job_id', None)
    assert job_id is not None
    cluster_name = request.values.get('cluster_name', None)
    assert cluster_name is not None
    subprocess.Popen([
        'python', '-m', 'sky.spot.controller',
        f'~/.sky/sky_spot_tasks/{cluster_name}.yaml', '--job-id',
        str(job_id)
    ])
    return 'ok'


@app.route('/next_job_id/', methods=['POST'])
def get_next_job_id():
    global global_job_id
    global_job_id += 1
    return str(global_job_id)


if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0', port=5000)
