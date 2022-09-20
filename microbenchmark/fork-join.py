import copy
import random
import string
import time

import sky

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
        source = create_task()
        sink = create_task()
        for _ in range(4):
            parent_task = source
            for _ in range(10):
                t = create_task()
                t.set_inputs(parent_task.get_outputs())
                parent_task >> t
                parent_task = t
            parent_task >> sink
    return dag


if __name__ == '__main__':
    random.seed(0)
    dag = make_application()
    copy_dag = copy.deepcopy(dag)

    sky.optimize(dag)

    start_time = time.time()
    sky.optimize(copy_dag, quiet=True)
    print(f'Optimized in {time.time() - start_time} seconds')
