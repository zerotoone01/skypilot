import sky
from sky.backends import docker_utils

if __name__ == '__main__':
    setup_command = 'echo hi'
    base_image = 'ubuntu:20.04'
    # The final dir is put in root
    work_dir = '/mnt/d/Romil/Berkeley/Research/sky-experiments/prototype/sky/'
    with sky.Dag():
        t = sky.Task(name="mytask",
                     workdir=work_dir,
                     setup=setup_command,
                     docker_image=base_image)
        tag = docker_utils.build_dockerimage_from_task(t)
    print(f"Build successful. Tag: {tag}.")
    print(f"docker run -it --rm {tag} /bin/bash")