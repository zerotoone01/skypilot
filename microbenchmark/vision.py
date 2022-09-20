import copy
import random
import time

import sky


def make_application():
    with sky.Dag() as dag:
        train = sky.Task('train')
        train.set_inputs({'s3://sky-imagenet-bucket': (150, 'us-west-1')})
        train.set_outputs({'CLOUD://skypilot-pipeline-model-xx': (0.1, None)})
        train.set_resources(
            {sky.Resources(accelerators='V100', disk_size=400, cloud=sky.AWS()),
            sky.Resources(instance_type='n1-standard-8', accelerators='tpu-v3-8', disk_size=400)})
        train_time = {'V100': random.random() * 3600, 'tpu-v3-8': random.random() * 3600}
        train.set_time_estimator(lambda r: train_time[list(r.accelerators.keys())[0]])


        infer = sky.Task('infer')
        infer.set_inputs(train.get_outputs())
        infer.set_resources({sky.Resources(instance_type='n1-standard-8', accelerators='tpu-v3-8'), sky.Resources(accelerators={'T4': 1}), sky.Resources(instance_type='inf1.2xlarge')})
        infer_time = {'tpu-v3-8': random.random() * 3600, 'T4': random.random() * 3600, 'Inferentia': random.random() * 3600}
        infer.set_time_estimator(lambda r: infer_time[list(r.accelerators.keys())[0]])

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
