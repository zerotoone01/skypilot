import copy
import random
import string
import time

import sky

CLOUDS = {
    'AWS': sky.AWS(),
    'GCP': sky.GCP(),
    'Azure': sky.Azure(),
}
CLOUD_TO_STORAGE = {
    'AWS': 's3:',
    'GCP': 'gs:',
    'Azure': 'azure:',
}

TASK_COUNTER = 0


def generate_random_name() -> str:
    return ''.join(random.choices(string.ascii_lowercase, k=10))


def create_task():
    global TASK_COUNTER

    time_estimate = {
        'AWS': random.random() * 3600,
        'GCP': random.random() * 3600,
        'Azure': random.random() * 3600,
    }
    task = sky.Task(f'task_{TASK_COUNTER}')
    task.set_resources({sky.Resources(cloud=sky.AWS()), sky.Resources(cloud=sky.GCP()), sky.Resources(cloud=sky.Azure())})
    task.set_time_estimator(lambda r: time_estimate[str(r.cloud)])
    output_name = generate_random_name()
    task.set_outputs({output_name: (random.randint(1, 100), None)})
    TASK_COUNTER += 1
    return task


def make_application():
    with sky.Dag() as dag:
        parent_task = None
        for _ in range(20):
            t = create_task()
            if parent_task is None:
                src_cloud = random.choice(['AWS', 'GCP', 'Azure'])
                src_region = random.choice(CLOUDS[src_cloud].regions())
                src_volume = random.randint(1, 100)
                input_name = generate_random_name()
                input_name = CLOUD_TO_STORAGE[src_cloud] + input_name
                t.set_inputs({input_name: (src_volume, src_region.name)})
            else:
                t.set_inputs(parent_task.get_outputs())
                parent_task >> t
            parent_task = t
    
    return dag


if __name__ == '__main__':
    random.seed(0)
    dag = make_application()
    copy_dag = copy.deepcopy(dag)

    sky.optimize(dag)

    start_time = time.time()
    sky.optimize(copy_dag, quiet=True)
    print(f'Optimized in {time.time() - start_time} seconds')
