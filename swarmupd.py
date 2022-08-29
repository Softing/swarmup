import os
import time
import sys
from multiprocessing import Pool
from subprocess import Popen, PIPE
import docker


def log(msg):
    print(msg)


def debug(msg):
    if SWARMUP_DEBUG:
        log(msg)


def exception_service(service, ex):
    print("%s: ERROR %s" % (service.name, ex))


def log_service(service, msg):
    print("%s: %s" % (service.name, msg))


def debug_service(service, msg):
    if SWARMUP_DEBUG:
        log_service(service, msg)


def login_to_registry():
    print("# LOGIN TO DOCKER REGISTRY")
    print("-" * 100)
    print("")

    if SWARMUP_REGISTRY_USER and SWARMUP_REGISTRY_PASSWORD:

        command = ["docker", "login"]

        if SWARMUP_REGISTRY_URL:
            command.append(SWARMUP_REGISTRY_URL)
            print(" - URL %s" % SWARMUP_REGISTRY_URL)

        command.append("--username")
        command.append(SWARMUP_REGISTRY_USER)
        print(" - User %s" % SWARMUP_REGISTRY_USER)

        command.append("--password-stdin")
        process = Popen(command, stdout=PIPE, stdin=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate(input=bytes(SWARMUP_REGISTRY_PASSWORD, 'utf-8'))

        if process.returncode == 0:
            print("\n Result: " + stdout.decode("utf-8"))

        if process.returncode == 1:
            sys.exit(stderr.decode("utf-8"))

    else:
        print(" We dont login to registry")


def find_service_config(client, prefix):
    result = None
    current_version = 0

    for config in client.configs.list():

        config_id = config.attrs['ID']
        config_name = config.attrs['Spec']['Name']
        content = config.attrs['Spec']['Data']
        version = config.attrs['Version']['Index']

        if config_name.startswith(prefix):
            if version > current_version:
                current_version = version
                result = {
                    "prefix": prefix,
                    "id": config_id,
                    "name": config_name,
                    "content": content,
                    "version": version
                }

    return result


def update_service_config(service_id, remove_config, config_name, config_path):
    command = ["docker", "service", "update"]

    if (remove_config):
        command.append("--config-rm")
        command.append(remove_config)

    command.append("--config-add")
    command.append("source=%s,target=%s" % (config_name, config_path))

    command.append(service_id)

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        print("\n Result: " + stdout.decode("utf-8"))

    if process.returncode == 1:
        sys.exit(stderr.decode("utf-8"))


def update_service_image(service_id, image_uri):
    command = ["docker", "service", "update"]

    if SWARMUP_REGISTRY_USER and SWARMUP_REGISTRY_PASSWORD:
        command.append("--with-registry-auth")

    if SWARMUP_DETACH_OPTION:
        command.append("--detach=false")

    if SWARMUP_INSECURE_REGISTRY:
        command.append("--insecure")

    if SWARMUP_NO_RESOLVE_IMAGE:
        command.append("--no-resolve-image")

    command.append("--image")
    command.append(image_uri)
    command.append(service_id)

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        debug("\n Result: " + stdout.decode("utf-8"))

    if process.returncode == 1:
        sys.exit(stderr.decode("utf-8"))


def process_configs(service_id):
    client = docker.from_env()
    service = client.services.get(service_id)

    service_id = service.id
    service_labels = service.attrs['Spec']['Labels']

    label_found = False

    for label_key, label_value in service_labels.items():

        if label_key.startswith(SWARMUP_CONFIG_LABEL + "."):

            label_found = True

            # current service configuration from labels
            label_config_prefix = label_key.removeprefix(SWARMUP_CONFIG_LABEL + ".")
            label_config_path = label_value

            debug(" - Label key: %s" % label_key)
            debug(" - Label prefix: %s" % SWARMUP_CONFIG_LABEL)
            debug(" - Label postfix: %s" % label_config_prefix)
            debug(" - Label value: %s" % label_value)

            # latest actual config
            swarm_config = find_service_config(client, label_config_prefix)

            if swarm_config:

                swarm_config_id = swarm_config['id']
                swarm_config_name = swarm_config['name']

                debug(" - Swarm config %s with name %s was found" % (swarm_config_id, swarm_config_name))

                # Make resolution

                resolution = {
                    "action": 'add',
                    "service_id": service_id,
                    "config_name": swarm_config_name,
                    "config_path": label_config_path,
                    "remove_config": None
                }

                service_configs = service.attrs.get('Spec', {}).get('TaskTemplate', {}).get('ContainerSpec', {}).get(
                    'Configs', [])

                for current_config in service_configs:

                    if current_config['ConfigName'].startswith(swarm_config['prefix']):

                        current_config_id = current_config['ConfigID']
                        current_config_name = current_config['ConfigName']
                        current_config_path = current_config['File']['Name']

                        debug(" - Service config %s with name %s was found" % (current_config_id, current_config_name))

                        if current_config_id == swarm_config_id:
                            resolution['action'] = 'none'

                        if current_config_id != swarm_config_id:
                            resolution['action'] = 'update'
                            resolution['remove_config'] = current_config_name

                        if current_config_path != label_config_path:
                            resolution['action'] = 'update'
                            resolution['remove_config'] = current_config_name

                # Resolve resolution

                if resolution['action'] == 'add':
                    log_service(service, "add config %s as %s" % (resolution['config_name'], resolution['config_path']))
                    update_service_config(
                        resolution['service_id'],
                        resolution['remove_config'],
                        resolution['config_name'],
                        resolution['config_path']
                    )
                if resolution['action'] == 'update':
                    log_service(service,
                                "update config %s as %s" % (resolution['config_name'], resolution['config_path']))
                    update_service_config(
                        resolution['service_id'],
                        resolution['remove_config'],
                        resolution['config_name'],
                        resolution['config_path']
                    )
                if resolution['action'] == 'none':
                    log_service(service, "bypass config")

            else:
                exception_service(service, "Unknown config %s" % label_config_prefix)

    if not label_found:
        debug(service, "Label %s.* not found!" % SWARMUP_CONFIG_LABEL)


def process_image(service_id):
    client = docker.from_env()
    service = client.services.get(service_id)

    service_id = service.id
    service_labels = service.attrs['Spec']['Labels']

    label_found = False

    for label_key, label_value in service_labels.items():

        if label_key.startswith(SWARMUP_IMAGE_LABEL):
            label_found = True
            debug(" - Label %s was found!" % SWARMUP_IMAGE_LABEL)

    if label_found:

        # Image
        service_image_with_hash = service.attrs['Spec']['TaskTemplate']['ContainerSpec']['Image'].split('@')

        # Get Image URI and Tag
        service_image = service_image_with_hash[0]
        service_image_uri = service_image.split(':')[0]
        service_image_tag = service_image.split(':')[1]

        # Get image SHA
        service_image_sha = ""

        if len(service_image_with_hash) == 2:
            service_image_sha = service_image_with_hash[1]

        debug(" - service_image_uri: " + service_image_uri)
        debug(" - service_image_tag: " + service_image_tag)
        debug(" - service_image_sha: " + service_image_sha)

        # Pull new image
        debug(" - Puling latest image...")
        try:
            client.images.pull(service_image_uri, service_image_tag)
        except Exception as ex:
            exception_service(service, ex)

        # Get registry image data
        swarm_image = client.images.get(service_image_uri)
        swarm_image_uri, swarm_image_sha = swarm_image.attrs['RepoDigests'][0].split('@')
        swarm_image_update_uri = '%s:%s@%s' % (swarm_image_uri, service_image_tag, swarm_image_sha)

        # Update image?
        if service_image_sha != swarm_image_sha:
            log_service(service, 'Update image to ' + swarm_image_update_uri + "\n")
            update_service_image(service_id, swarm_image_update_uri)
        else:
            log_service(service, 'Newest image not found!\n')

        debug("-" * 100)
    else:
        debug(service, "Label %s not found!" % SWARMUP_IMAGE_LABEL)


# Common
SWARMUP_DEBUG = os.getenv("SWARMUP_DEBUG", None)
SWARMUP_TIMEOUT = os.getenv("SWARMUP_TIMEOUT", 30 * 1)
SWARMUP_CONFIG_LABEL = os.getenv("SWARMUP_CONFIG_LABEL", "swarmup.config")
SWARMUP_IMAGE_LABEL = os.getenv("SWARMUP_IMAGE_LABEL", "swarmup.image")

# Registry settings
SWARMUP_REGISTRY_URL = os.getenv("SWARMUP_REGISTRY_URL", None)
SWARMUP_REGISTRY_USER = os.getenv("SWARMUP_REGISTRY_USER", None)
SWARMUP_REGISTRY_PASSWORD = os.getenv("SWARMUP_REGISTRY_PASSWORD", None)

# Update settings
SWARMUP_DETACH_OPTION = os.getenv("SWARMUP_DETACH_OPTION", None)
SWARMUP_INSECURE_REGISTRY = os.getenv("SWARMUP_INSECURE_REGISTRY", None)
SWARMUP_NO_RESOLVE_IMAGE = os.getenv("SWARMUP_NO_RESOLVE_IMAGE", None)


def main():
    print("-" * 100)
    print("STARTUP VARIABLES")
    print("-" * 100)
    print("")
    print("SWARMUP_TIMEOUT %s" % SWARMUP_TIMEOUT)
    print("SWARMUP_CONFIG_LABEL %s" % SWARMUP_CONFIG_LABEL)
    print("SWARMUP_IMAGE_LABEL %s" % SWARMUP_IMAGE_LABEL)
    print("SWARMUP_REGISTRY_URL %s" % SWARMUP_REGISTRY_URL)
    print("SWARMUP_REGISTRY_USER %s" % SWARMUP_REGISTRY_USER)
    print("SWARMUP_REGISTRY_PASSWORD %s" % SWARMUP_REGISTRY_PASSWORD)
    print("SWARMUP_DETACH_OPTION %s" % SWARMUP_DETACH_OPTION)
    print("SWARMUP_INSECURE_REGISTRY %s" % SWARMUP_INSECURE_REGISTRY)
    print("SWARMUP_NO_RESOLVE_IMAGE %s" % SWARMUP_NO_RESOLVE_IMAGE)

    while (True):
        print("")
        print("@" * 100)
        print("@ START NEW CICLE")
        print("@" * 100)
        print("")

        login_to_registry()

        client = docker.from_env()

        print("# START UPDATE PROCESS...")
        print("-" * 100)
        services_images = []
        services_configs = []
        for service in client.services.list():
            print("## Service %s" % service.name)
            for label_key, label_value in service.attrs['Spec']['Labels'].items():
                if label_key.startswith(SWARMUP_IMAGE_LABEL):
                    services_images.append(service.id)
                    print(" - Image Label %s was found!" % SWARMUP_IMAGE_LABEL)
                if label_key.startswith(SWARMUP_CONFIG_LABEL + "."):
                    services_configs.append(service.id)
                    print(" - Config Label %s was found!" % SWARMUP_IMAGE_LABEL)

            services_images = list(dict.fromkeys(services_images))
            services_configs = list(dict.fromkeys(services_configs))

        print("\n")
        print("# OUTPUT IMAGES...")
        print("-" * 100)

        pool_images = Pool()

        for service_id in services_images:
            pool_images.apply_async(process_image, (service_id,))

        pool_images.close()
        pool_images.join()
        pool_images.terminate()

        print("\n")
        print("# OUTPUT CONFIGS...")
        print("-" * 100)

        pool_configs = Pool()
        for service_id in services_configs:
            pool_configs.apply_async(process_configs, (service_id,))

        pool_configs.close()
        pool_configs.join()
        pool_configs.terminate()

        time.sleep(int(SWARMUP_TIMEOUT))


if __name__ == '__main__':
    main()
