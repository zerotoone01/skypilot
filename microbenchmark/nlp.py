import copy
import random
import time

import sky


def make_application():
    with sky.Dag() as dag:
        data_proc = sky.Task('data_proc')
        data_proc.set_inputs({'gs:': (1, 'us-west1')})
        data_proc.set_outputs({'CLOUD://skypilot-pipeline-b-model': (1, None)})
        data_proc.set_resources({sky.Resources(instance_type='Standard_DC8_v2')})
        data_proc_time = random.random() * 3600
        data_proc.set_time_estimator(lambda _: data_proc_time)

        train = sky.Task('train')
        train.set_inputs(data_proc.get_outputs())
        train.set_outputs({'CLOUD://skypilot-pipeline-b-model': (0.1, None)})
        train.set_resources({sky.Resources(accelerators={'V100': 4}), sky.Resources(instance_type='n1-standard-8', accelerators='tpu-v3-8')})
        train_time = {'V100': random.random() * 3600, 'tpu-v3-8': random.random() * 3600}
        train.set_time_estimator(lambda r: train_time[list(r.accelerators.keys())[0]])


        infer = sky.Task('infer')
        infer.set_inputs(train.get_outputs())
        infer.set_resources({sky.Resources(accelerators={'T4': 1}), sky.Resources(instance_type='inf1.2xlarge')})
        infer_time = {'T4': random.random() * 3600, 'Inferentia': random.random() * 3600}
        infer.set_time_estimator(lambda r: infer_time[list(r.accelerators.keys())[0]])

        data_proc >> train
        train >> infer
    return dag


if __name__ == '__main__':
    random.seed(0)
    dag = make_application()
    copy_dag = copy.deepcopy(dag)

    sky.optimize(dag)

    start_time = time.time()
    sky.optimize(copy_dag, quiet=True)
    print(f'Optimized in {time.time() - start_time} seconds')
