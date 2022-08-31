import docker
import os
import sys
import time
from multiprocessing import Pool
from subprocess import Popen, PIPE


def log(msg):
    print(msg)


def debug(msg):
    if SWARMUP_DEBUG:
        log(msg)


def exception_service(service, ex):
    print("ERROR: %s - %s" % (service.name, ex))


def log_service(service, msg):
    print("INFO: %s - %s" % (service.name, msg))


def debug_service(service, msg):
    if SWARMUP_DEBUG:
        log_service(service, msg)


def login_to_registry():
    if SWARMUP_REGISTRY_USER and SWARMUP_REGISTRY_PASSWORD:

        command = ["docker", "login"]

        if SWARMUP_REGISTRY_URL:
            command.append(SWARMUP_REGISTRY_URL)
            print("Registry: '%s'" % SWARMUP_REGISTRY_URL)

        command.append("--username")
        command.append(SWARMUP_REGISTRY_USER)

        print("User: '%s'" % SWARMUP_REGISTRY_USER)

        command.append("--password-stdin")
        process = Popen(command, stdout=PIPE, stdin=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate(input=bytes(SWARMUP_REGISTRY_PASSWORD, 'utf-8'))

        if process.returncode == 0:
            print(stdout.decode("utf-8").rstrip())

        if process.returncode == 1:
            sys.exit(stderr.decode("utf-8"))

    else:
        print("We don't need to login to the registry")


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


def update_service_config(service, remove_config, config_name, config_path):
    command = ["docker", "service", "update"]

    if (remove_config):
        command.append("--config-rm")
        command.append(remove_config)

    command.append("--config-add")
    command.append("source=%s,target=%s" % (config_name, config_path))

    command.append(service.id)

    log_service(service, ' '.join(command))

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        debug_service(service, stdout.decode("utf-8").rstrip())

    if process.returncode == 1:
        exception_service(service, stderr.decode("utf-8").rstrip())


def update_service_image(service, image_uri):
    command = ["docker", "service", "update"]

    if SWARMUP_WITH_REGISTRY_AUTH or (SWARMUP_REGISTRY_USER and SWARMUP_REGISTRY_PASSWORD):
        command.append("--with-registry-auth")

    if SWARMUP_DETACH:
        command.append("--detach=false")

    if SWARMUP_INSECURE:
        command.append("--insecure")

    if SWARMUP_NO_RESOLVE_IMAGE:
        command.append("--no-resolve-image")

    command.append("--image")
    command.append(image_uri)
    command.append(service.id)

    log_service(service, ' '.join(command))

    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        debug_service(service, stdout.decode("utf-8").rstrip())

    if process.returncode == 1:
        exception_service(service, stderr.decode("utf-8").rstrip())


def process_configs(service_id):
    client = docker.from_env()
    service = client.services.get(service_id)
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
                    "service": service,
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
                    log_service(service, "New config '%s' found" % (resolution['config_name']))
                    log_service(service, "Add config '%s' at %s" % (resolution['config_name'], resolution['config_path']))
                    update_service_config(
                        resolution['service'],
                        resolution['remove_config'],
                        resolution['config_name'],
                        resolution['config_path']
                    )
                    log_service(service, "Config '%s' added!" % resolution['config_name'])

                if resolution['action'] == 'update':
                    log_service(service, "Config '%s' updates found" % (resolution['config_name']))
                    log_service(service, "Update config '%s' at %s" % (resolution['config_name'], resolution['config_path']))
                    update_service_config(
                        resolution['service'],
                        resolution['remove_config'],
                        resolution['config_name'],
                        resolution['config_path']
                    )
                    log_service(service, "Config '%s' updated!" % resolution['config_name'])

                if resolution['action'] == 'none':
                    log_service(service, "No config updates found!")

            else:
                exception_service(service, "The config with the name '%s' does not exist" % label_config_prefix)

    if not label_found:
        debug_service(service, "Label %s.* not found!" % SWARMUP_CONFIG_LABEL)


def process_image(service_id):
    client = docker.from_env()
    service = client.services.get(service_id)

    service_labels = service.attrs['Spec']['Labels']

    label_found = False

    for label_key, label_value in service_labels.items():
        if label_key.startswith(SWARMUP_IMAGE_LABEL):
            label_found = True

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

        # log_service(service, "service_image_uri: " + service_image_uri)
        # log_service(service, "service_image_tag: " + service_image_tag)
        # log_service(service, "service_image_sha: " + service_image_sha)

        # Pull new image
        debug_service(service, "Getting information about image updates...")

        try:
            client.images.pull(service_image_uri, service_image_tag)
        except Exception as ex:
            exception_service(service, ex)

        # Get registry image data
        swarm_image = client.images.get(service_image_uri)
        swarm_image_uri, swarm_image_sha = swarm_image.attrs['RepoDigests'][0].split('@')
        swarm_image_update_uri = '%s:%s' % (swarm_image_uri, service_image_tag)

        # Update image?
        if service_image_sha != swarm_image_sha:
            log_service(service, 'Update found!')
            log_service(service, 'Update image to ' + swarm_image_update_uri)
            update_service_image(service, swarm_image_update_uri)
            log_service(service, 'Updated!')
        else:
            log_service(service, 'No image updates found!')

        debug("-" * 100)
    else:
        debug_service(service, "Label %s not found!" % SWARMUP_IMAGE_LABEL)


# Common
SWARMUP_DEBUG = os.getenv("SWARMUP_DEBUG", None)
SWARMUP_TIMEOUT = os.getenv("SWARMUP_TIMEOUT", 10 * 1)
SWARMUP_CONFIG_LABEL = os.getenv("SWARMUP_CONFIG_LABEL", "swarmup.config")
SWARMUP_IMAGE_LABEL = os.getenv("SWARMUP_IMAGE_LABEL", "swarmup.image")

# Registry settings
SWARMUP_REGISTRY_URL = os.getenv("SWARMUP_REGISTRY_URL", None)
SWARMUP_REGISTRY_USER = os.getenv("SWARMUP_REGISTRY_USER", None)
SWARMUP_REGISTRY_PASSWORD = os.getenv("SWARMUP_REGISTRY_PASSWORD", None)
SWARMUP_WITH_REGISTRY_AUTH = os.getenv("SWARMUP_WITH_REGISTRY_AUTH", None)

# Update settings
SWARMUP_DETACH = os.getenv("SWARMUP_DETACH", None)
SWARMUP_INSECURE = os.getenv("SWARMUP_INSECURE", None)
SWARMUP_NO_RESOLVE_IMAGE = os.getenv("SWARMUP_NO_RESOLVE_IMAGE", None)


def main():
    print("# STARTUP VARIABLES")
    print("-" * 100)
    print("# SWARMUP_TIMEOUT: %s" % SWARMUP_TIMEOUT)
    print("# SWARMUP_CONFIG_LABEL: %s" % SWARMUP_CONFIG_LABEL)
    print("# SWARMUP_IMAGE_LABEL: %s" % SWARMUP_IMAGE_LABEL)
    print("# SWARMUP_REGISTRY_URL: %s" % SWARMUP_REGISTRY_URL)
    print("# SWARMUP_REGISTRY_USER: %s" % SWARMUP_REGISTRY_USER)
    print("# SWARMUP_REGISTRY_PASSWORD: %s" % SWARMUP_REGISTRY_PASSWORD)
    print("# SWARMUP_WITH_REGISTRY_AUTH: %s" % SWARMUP_WITH_REGISTRY_AUTH)
    print("# SWARMUP_DETACH: %s" % SWARMUP_DETACH)
    print("# SWARMUP_INSECURE: %s" % SWARMUP_INSECURE)
    print("# SWARMUP_NO_RESOLVE_IMAGE: %s" % SWARMUP_NO_RESOLVE_IMAGE)

    while (True):

        print("")
        print("# START NEW CYCLE")
        print("-" * 100)

        print("")
        print("## LOGIN TO REGISTRY")
        print("-" * 100)

        login_to_registry()

        client = docker.from_env()

        print("")
        print("## SEARCH SERVICES...")
        print("-" * 100)

        services_images = []
        services_configs = []
        for service in client.services.list():
            # print("## Service %s" % service.name)
            for label_key, label_value in service.attrs['Spec']['Labels'].items():
                if label_key.startswith(SWARMUP_IMAGE_LABEL):
                    services_images.append(service.id)
                    log_service(service, "Service with the label '%s' found" % label_key)
                if label_key.startswith(SWARMUP_CONFIG_LABEL + "."):
                    services_configs.append(service.id)
                    log_service(service, "Service with the label '%s.xxx' found" % label_key)

            services_images = list(dict.fromkeys(services_images))
            services_configs = list(dict.fromkeys(services_configs))

        if len(services_images) == 0:
            print("Services with the label '%s' were not found" % SWARMUP_IMAGE_LABEL)

        if len(services_configs) == 0:
            print("Services with the label '%s.xxx' were not found" % SWARMUP_CONFIG_LABEL)

        print("")
        print("## PROCESS IMAGES...")
        print("-" * 100)

        if len(services_images):

            pool_images = Pool()

            for service_id in services_images:
                pool_images.apply_async(process_image, (service_id,))

            pool_images.close()
            pool_images.join()
            pool_images.terminate()
        else:
            print("There are no services with a label '%s'" % SWARMUP_IMAGE_LABEL)

        print("")
        print("## PROCESS CONFIGS...")
        print("-" * 100)

        if len(services_configs):
            pool_configs = Pool()
            for service_id in services_configs:
                pool_configs.apply_async(process_configs, (service_id,))

            pool_configs.close()
            pool_configs.join()
            pool_configs.terminate()
        else:
            print("There are no services with a label '%s.xxx'" % SWARMUP_CONFIG_LABEL)

        print("")
        print("Timeout for %d seconds" % int(SWARMUP_TIMEOUT))
        time.sleep(int(SWARMUP_TIMEOUT))


if __name__ == '__main__':
    main()
