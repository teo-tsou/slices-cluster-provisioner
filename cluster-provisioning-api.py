# Cluster provisioner Library v1.0
# Python Script that automates the Rancher, KubeVirt, and CDI procedures, by taking advantage of the Rancher and K8s APIs.   
# Author: Theodoros Tsourdinis
# email:  theodoros.tsourdinis@sorbonne-universite.fr


import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.auth import HTTPBasicAuth
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from kubernetes import config, client
from kubernetes.stream import stream
import time
from kubernetes.client.rest import ApiException
import urllib3
import asyncio
import yaml
import ipaddress
import base64
import os

# RANCHER Client Credentials
RANCHER_URL = 'https://172.28.2.88:443'
RANCHER_ACCESS_KEY = 'token-g8fdl'
RANCHER_SECRET_KEY = 'lrpfx6tmsph7rtw4c2n88tdf769d2l7wtmfk9vw7q9tkjcjjk8lmn9'
VERIFY_SSL = False

# Load Kubernetes config and client instance
config.load_kube_config()
api_instance = client.CustomObjectsApi()


#######################################
###### K8s Cluster-related Functions #####
#######################################

def create_namespace(namespace):
    """
    Creates a Kubernetes namespace if it does not already exist.

    Args:
        namespace (str): The name of the namespace to create.

    Returns:
        The created V1Namespace object on success, or None if an error occurred.
    """
    # Load Kubernetes configuration (from ~/.kube/config)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ns_body = client.V1Namespace(
        metadata=client.V1ObjectMeta(name=namespace)
    )
    try:
        # Try to create the namespace
        ns = v1.create_namespace(body=ns_body)
        print(f"[SUCCESS] Namespace '{namespace}' created.")
        return ns
    except ApiException as e:
        if e.status == 409:
            print(f"[INFO] Namespace '{namespace}' already exists.")
            return 409
        else:
            print(f"[ERROR] Failed to create namespace '{namespace}': {e}")
        return None


def delete_namespace(namespace):
    """
    Deletes a Kubernetes namespace.

    Args:
        namespace (str): The name of the namespace to delete.

    Returns:
        The response from the API server on success, or None if an error occurred.
    """
    # Load Kubernetes configuration
    config.load_kube_config()
    v1 = client.CoreV1Api()
    try:
        resp = v1.delete_namespace(name=namespace)
        print(f"[SUCCESS] Namespace '{namespace}' deletion initiated.")
        return resp
    except ApiException as e:
        if e.status == 404:
            print(f"[INFO] Namespace '{namespace}' not found; nothing to delete.")
        else:
            print(f"[ERROR] Failed to delete namespace '{namespace}': {e}")
        return None


def select_non_overlapping_cidr(default_cidr_str, parent_cidr_str, delta=10):
    """
    Given a default CIDR (assumed in the form '10.X.0.0/16') and a parent's CIDR,
    return a candidate CIDR that does NOT overlap with the parent's CIDR.
    If the default candidate overlaps, the function increases the second octet by delta
    until a non-overlapping candidate is found.
    
    Args:
        default_cidr_str (str): e.g., "10.50.0.0/16"
        parent_cidr_str (str): parent's CIDR (e.g., "10.42.0.0/24" or "10.43.0.0/16")
        delta (int): increment for the second octet (default: 10)
        
    Returns:
        str: A new CIDR that does not overlap with the parent's CIDR.
    """
    parent_net = ipaddress.ip_network(parent_cidr_str, strict=False)
    candidate = ipaddress.ip_network(default_cidr_str, strict=False)
    
    # Loop until candidate does NOT overlap with the parent's network.
    while candidate.overlaps(parent_net):
        # Parse the candidate, assumed to be in the form "10.<X>.0.0/16"
        parts = default_cidr_str.split('.')
        new_second = int(parts[1]) + delta
        default_cidr_str = f"{parts[0]}.{new_second}.0.0/16"
        candidate = ipaddress.ip_network(default_cidr_str, strict=False)
    return str(candidate)

def get_new_cidrs():
    """
    Loads the kubeconfig and retrieves the parent's Pod CIDR (from one node) and infers
    the parent's Service CIDR (from a DNS service's ClusterIP). It then prints the parent's
    CIDRs and returns new CIDRs according to the following rule:
    
      - Default new Pod CIDR: 10.50.0.0/16  
      - Default new Service CIDR: 10.51.0.0/16
    
    If a default candidate overlaps with the parent CIDR, the function selects an alternative
    by increasing the second octet (e.g., to 10.60.0.0/16, 10.70.0.0/16, etc.).
    
    Returns:
        tuple: (new_pod_cidr, new_service_cidr) as separate strings.
    """
    # Load kubeconfig and create API client.
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    # Retrieve the parent's Pod CIDR from the first node with one.
    nodes = v1.list_node().items
    original_pod_cidr = None
    for node in nodes:
        if node.spec.pod_cidr:
            original_pod_cidr = node.spec.pod_cidr
            break
    if not original_pod_cidr:
        raise RuntimeError("No pod CIDR found in the cluster nodes.")
    
    # Try to get the DNS service from kube-system (try "coredns", then "kube-dns").
    dns_service = None
    for svc_name in ["coredns", "kube-dns"]:
        try:
            dns_service = v1.read_namespaced_service(svc_name, "kube-system")
            break
        except client.exceptions.ApiException as e:
            if e.status != 404:
                raise RuntimeError(f"Error retrieving {svc_name} service: {e}")
    # Fallback: use the "kubernetes" service in the "default" namespace.
    if not dns_service:
        try:
            dns_service = v1.read_namespaced_service("kubernetes", "default")
        except client.exceptions.ApiException as e:
            raise RuntimeError("Could not retrieve a DNS service ('coredns' or 'kube-dns') from kube-system, "
                               "and fallback 'kubernetes' service from default failed: " + str(e))
    
    original_service_ip = dns_service.spec.cluster_ip
    # Infer the parent's Service CIDR as /16 based on the first two octets of the ClusterIP.
    octets = original_service_ip.split('.')
    if len(octets) != 4:
        raise RuntimeError("Invalid ClusterIP: " + original_service_ip)
    original_service_cidr = f"{octets[0]}.{octets[1]}.0.0/16"
    
    # Set defaults.
    default_pod_cidr = "10.50.0.0/16"
    default_service_cidr = "10.51.0.0/16"
    
    # Check for overlaps and adjust if necessary.
    new_pod_cidr = select_non_overlapping_cidr(default_pod_cidr, original_pod_cidr)
    new_service_cidr = select_non_overlapping_cidr(default_service_cidr, original_service_cidr)
    
    return new_pod_cidr, new_service_cidr


def get_k8s_node_ips():
    """
    Retrieves a list of all Kubernetes node IP addresses using Kubernetes Python API.

    Returns:
      - List[str]: List of node IP addresses.
    """
    # Load Kubernetes configuration
    config.load_kube_config()

    v1 = client.CoreV1Api()

    try:
        nodes = v1.list_node()
        node_ips = []

        for node in nodes.items:
            for address in node.status.addresses:
                if address.type == "InternalIP":
                    node_ips.append(address.address)

        return node_ips

    except client.exceptions.ApiException as e:
        print(f"[ERROR] Failed to retrieve nodes: {e}")
        return []


#######################################
###### Rancher Related Functions ######
#######################################

def list_all_clusters_from_provisioning_api():
    """
    Retrieves all clusters from the Rancher provisioning API endpoint.
    
    Returns:
      - list: A list of dictionaries containing 'Cluster Name' and 'Cluster ID'.
      - None: If the request fails.
    """
    endpoint = f"{RANCHER_URL.rstrip('/')}/v1/provisioning.cattle.io.clusters"
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(endpoint, headers=headers, verify=VERIFY_SSL)
        response.raise_for_status()
        data = response.json()
        
        # The clusters are expected to be in the "data" key
        clusters = data.get("data", [])
        cluster_details = []
        
        for cluster in clusters:
            # For provisioning API objects, the name is usually in metadata
            name = cluster.get("metadata", {}).get("name")
            # The cluster id might be at the top level or inside metadata, adjust as needed
            cluster_id = cluster.get("id", "N/A")
            cluster_details.append({
                "Cluster Name": name,
                "Cluster ID": cluster_id
            })
        
        return cluster_details
    
    except Exception as e:
        print(f"[ERROR] Failed to list clusters: {e}")
        return None


def create_custom_cluster(
    cluster_name="my-custom-cluster",
    cluster_type="rke2",
    kubernetes_version="v1.31.5+rke2r1",
    namespace="fleet-default",
    cni="calico",
    pod_cidr="10.52.0.0/16",
    service_cidr="10.53.0.0/16",
    tls_san=None
):
    """
    Creates a Custom cluster in Rancher using the provisioning API (v1).
    Supports both RKE2 and K3s provisioning, with an option to configure an Authorized Endpoint.
    """

    if not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    provisioning_endpoint = f"{RANCHER_URL.rstrip('/')}/v1/provisioning.cattle.io.clusters"
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    base_spec = {
        "kubernetesVersion": kubernetes_version
    }

    config_key = "rkeConfig"

    if cluster_type.lower() == "rke2":
        base_spec["cni"] = cni
        base_spec[config_key] = {
            "machinePools": [],
            "machineGlobalConfig": {
                "cni": cni,
                "cluster-cidr": pod_cidr,
                "service-cidr": service_cidr,
                "tls-san": tls-san
            }
        }
    elif cluster_type.lower() == "k3s":
        base_spec[config_key] = {
            "machinePools": [],
            "machineGlobalConfig": {
                "cluster-cidr": pod_cidr,
                "service-cidr": service_cidr,
                "tls-san": tls_san
            }
        }
    else:
        raise ValueError("Invalid cluster_type. Must be 'rke2' or 'k3s'.")

    payload = {
        "apiVersion": "provisioning.cattle.io/v1",
        "kind": "Cluster",
        "metadata": {
            "name": cluster_name,
            "namespace": namespace
        },
        "spec": base_spec
    }

    # POST to Rancher
    resp = requests.post(
        provisioning_endpoint,
        json=payload,
        headers=headers,
        verify=VERIFY_SSL
    )

    if resp.status_code == 201:
        print(f"[SUCCESS] Created {cluster_type.upper()} custom cluster '{cluster_name}' in '{namespace}'.")
        return resp.json()
    elif resp.status_code == 409:
        print(f"[ERROR] Failed to create {cluster_type.upper()} cluster. Cluster already exists.")
        return resp.status_code
    else:
        print(f"[ERROR] Failed to create {cluster_type.upper()} cluster. HTTP {resp.status_code}")
        print("Response:", resp.text)
        return None


def get_cluster_state(cluster_name, namespace="fleet-default"):
    """
    Retrieves the state of a Rancher cluster (provisioned via the Rancher provisioning API)
    based on the cluster's name and namespace.
    
    This version attempts to find the "Ready" condition in the cluster's status.conditions array.
    
    Args:
      - cluster_name (str): The name of the cluster.
      - namespace (str): The namespace in which the cluster resides (typically "fleet-default").
      
    Returns:
      - str: The state of the cluster (for example, the message from the "Ready" condition)
             or "Unknown" if not found.
      - None: if the HTTP request fails.
    """
    endpoint = f"{RANCHER_URL.rstrip('/')}/v1/provisioning.cattle.io.clusters/{namespace}/{cluster_name}"
    
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(endpoint, headers=headers, verify=VERIFY_SSL)
        response.raise_for_status()
        cluster_obj = response.json()
        
        # For debugging, you might uncomment the next line to inspect the full JSON:
        # print(cluster_obj)
        
        # Look for a condition of type "Ready" in the status.conditions array
        conditions = cluster_obj.get("status", {}).get("conditions", [])
        for condition in conditions:
            if condition.get("type") == "Ready":
                # You might return the message, or the status, or combine both
                message = condition.get("message")
                status = condition.get("status")
                # For example, if the cluster is ready, status might be "True"
                return f"Ready: {status}, {message}"
        
        # Fallback: Try checking for a top-level "phase" or "state"
        state = cluster_obj.get("status", {}).get("phase") or cluster_obj.get("status", {}).get("state")
        if state:
            return state
        
        return "Unknown"
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to retrieve cluster state for '{cluster_name}' in namespace '{namespace}': {e}")
        return None


def list_cluster_nodes_v3(cluster_id):
    """
    Retrieves the list of nodes for a given cluster using the Rancher v3 API.
    
    Uses the endpoint:
      /v3/clusters/{cluster_id}/nodes
      
    Args:
      - cluster_id (str): The internal ID of the cluster (e.g., "c-m-bzs4zgtr").
      
    Returns:
      - list: A list of node objects (dictionaries) as returned by the API.
              Returns an empty list if no nodes are found or on error.
    """
    endpoint = f"{RANCHER_URL.rstrip('/')}/v3/clusters/{cluster_id}/nodes"
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(endpoint, headers=headers, verify=VERIFY_SSL)
        response.raise_for_status()
        data = response.json()
        nodes = data.get("data", [])
        print(f"[INFO] Found {len(nodes)} node(s) for cluster '{cluster_id}'.")
        return nodes
    except Exception as e:
        print(f"[ERROR] Failed to list nodes for cluster '{cluster_id}': {e}")
        return []


# This function getting the internal cluster name 
def get_internal_cluster_name(provisioning_namespace, provisioning_cluster_name):
    """
    Fetch the provisioning.cattle.io/v1 cluster and see what management cluster name
    it is mapped to. Usually found in status.clusterName.
    """
    if not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    prov_endpoint = f"{RANCHER_URL.rstrip('/')}/v1/provisioning.cattle.io.clusters/{provisioning_namespace}/{provisioning_cluster_name}"
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    resp = requests.get(prov_endpoint, headers=headers, verify=VERIFY_SSL)
    if resp.status_code == 200:
        obj = resp.json()
        # Typically it's in something like obj["status"]["clusterName"] or an annotation
        status = obj.get("status", {})
        mgmt_name = status.get("clusterName")
        if not mgmt_name:
            print("No 'clusterName' in the provisioning cluster status. Possibly still initializing.")
        return mgmt_name
    else:
        print(f"[ERROR] Failed to GET provisioning cluster. {resp.status_code}: {resp.text}")
        return None


async def wait_for_internal_cluster_name(rancher_namespace, cluster_name, timeout=600, poll_interval=1):
    """
    Polls until the internal cluster name is available for the given cluster.
    
    Args:
      - rancher_namespace (str): The namespace where the cluster is created.
      - cluster_name (str): The name of the cluster.
      - timeout (int): Maximum time to wait (in seconds).
      - poll_interval (int): Time between polls (in seconds).
    
    Returns:
      - str: The internal cluster name if it becomes available within the timeout.
      - None: Otherwise.
    """
    start = time.time()
    while time.time() - start < timeout:
        internal_cluster_id = await asyncio.to_thread(get_internal_cluster_name, rancher_namespace, cluster_name)
        if internal_cluster_id:
            return internal_cluster_id
        print("[INFO] Internal cluster name not yet available. Waiting for {} seconds...".format(poll_interval))
        await asyncio.sleep(poll_interval)
    return None


def get_rancher_registration_command(cluster_name):
    """
    Fetch the insecureNodeCommand (registration command) for a specific Rancher cluster.
    
    Args:
        cluster_name (str): The name of the management cluster (e.g., 'c-m-b4zrqdjm').

    Returns:
        str: The node registration command or None if not found.
    """
    # API endpoint for Cluster Registration Tokens
    endpoint = f"{RANCHER_URL}/v1/management.cattle.io.clusterregistrationtokens?namespace={cluster_name}"
    
    # Set up authentication
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    # Make the request
    try:
        response = requests.get(endpoint, headers=headers, verify=VERIFY_SSL)
        response.raise_for_status()  # Raise an error if request fails
        data = response.json()

        # Extract the correct token
        for item in data.get("data", []):
            if item.get("spec", {}).get("clusterName") == cluster_name:
                command = item.get("status", {}).get("insecureNodeCommand")
                if command:
                    return command

        print(f"[INFO] No registration command found for cluster '{cluster_name}'.")
        return None
    
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")
        return None


def wait_for_registration_command(internal_cluster_id, timeout=300, interval=2):
    """
    Polls until a valid registration command is available for the cluster.
    
    Args:
      - internal_cluster_id (str): The internal ID of the cluster.
      - timeout (int): Maximum number of seconds to wait.
      - interval (int): Polling interval in seconds.
      
    Returns:
      - str: The valid registration command once available, or an empty string if timeout is reached.
    """
    start = time.time()
    reg_cmd = get_rancher_registration_command(internal_cluster_id)
    while (not reg_cmd or reg_cmd.strip() == "") and (time.time() - start < timeout):
        print("[INFO] Registration command not yet available. Waiting...")
        time.sleep(interval)
        reg_cmd = get_rancher_registration_command(internal_cluster_id)
    return reg_cmd


def delete_provisioned_cluster(cluster_name, namespace="fleet-default"):
    """
    Deletes a Rancher cluster (RKE2 or K3s) created via the new provisioning API.
    :param cluster_name: The name of the cluster in the `namespace` (e.g. "my-k3s").
    :param namespace: The Fleet workspace namespace (often "fleet-default").
    :return: True if delete was successful, otherwise False.
    """

    # If using a self-signed certificate and want to skip verification
    if not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # DELETE /v1/provisioning.cattle.io.clusters/{namespace}/{cluster_name}
    delete_endpoint = f"{RANCHER_URL.rstrip('/')}/v1/provisioning.cattle.io.clusters/{namespace}/{cluster_name}"

    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.delete(delete_endpoint, headers=headers, verify=VERIFY_SSL)
        if resp.status_code in [200, 202, 204]:
            print(f"[SUCCESS] Deleted cluster '{cluster_name}' in '{namespace}'.")
            return True
        else:
            print(f"[ERROR] Failed to delete cluster. Status: {resp.status_code}, Response: {resp.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Exception while deleting cluster '{cluster_name}': {e}")
        return False


#######################################
###### KubeVirt/CDI Related Functions #
#######################################


def create_data_volume(dv_name, image_url, storage_size="10Gi", storage_class="local-path", namespace="default"):
    """
    Creates a CDI DataVolume to import an ISO/QCOW2 image into a PVC.
    
    Args:
    - dv_name (str): Name of the DataVolume
    - image_url (str): URL of the QCOW2 or ISO image
    - storage_size (str): Size of the storage (e.g., "10Gi")
    - storage_class (str): Storage class (e.g., "local-path")
    - namespace (str): Kubernetes namespace where DV will be created
    
    Returns:
    - str: Name of the created PVC (same as the DataVolume name)
    - None: If creation fails
    """
    
    dv_manifest = {
        "apiVersion": "cdi.kubevirt.io/v1beta1",
        "kind": "DataVolume",
        "metadata": {"name": dv_name, "namespace": namespace},
        "spec": {
            "source": {"http": {"url": image_url}},
            "pvc": {
                "storageClassName": storage_class,
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": storage_size}}
            }
        }
    }

    try:
        api_instance.create_namespaced_custom_object(
            group="cdi.kubevirt.io",
            version="v1beta1",
            namespace=namespace,
            plural="datavolumes",
            body=dv_manifest
        )
        print(f"[SUCCESS] DataVolume '{dv_name}' created successfully! (PVC: {dv_name})")
        return dv_name  # The PVC has the same name as the DataVolume
    except Exception as e:
        print(f"[ERROR] Failed to create DataVolume '{dv_name}': {e}")
        return None


def delete_data_volume(dv_name, namespace="default"):
    """
    Deletes a CDI DataVolume (DV) and its associated PersistentVolumeClaim (PVC).
    
    Args:
    - dv_name (str): Name of the DataVolume to delete.
    - namespace (str): Kubernetes namespace where the DV is located.
    
    Returns:
    - bool: True if the deletion was successful, False otherwise.
    """
    api_instance = client.CustomObjectsApi()
    core_api_instance = client.CoreV1Api()  # Correct API for PVCs

    try:
        # Delete the DataVolume
        api_instance.delete_namespaced_custom_object(
            group="cdi.kubevirt.io",
            version="v1beta1",
            namespace=namespace,
            plural="datavolumes",
            name=dv_name
        )
        print(f"[SUCCESS] DataVolume '{dv_name}' deleted successfully.")

    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"[INFO] DataVolume '{dv_name}' not found. It may have already been deleted.")
        else:
            print(f"[ERROR] Failed to delete DataVolume '{dv_name}': {e}")

    try:
        # Delete the corresponding PVC (PVC has the same name as the DV)
        core_api_instance.delete_namespaced_persistent_volume_claim(
            name=dv_name,
            namespace=namespace
        )
        print(f"[SUCCESS] PersistentVolumeClaim '{dv_name}' deleted successfully.")
        return True

    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"[INFO] PersistentVolumeClaim '{dv_name}' not found. It may have already been deleted.")
            return False
        print(f"[ERROR] Failed to delete PersistentVolumeClaim '{dv_name}': {e}")
        return False


def create_network_attachment_definitions(multus_configs, namespace="default"):
    """
    Creates NetworkAttachmentDefinitions for all provided Multus configurations.
    
    Args:
    - multus_configs (list): A list of dictionaries, each with keys:
         - network_name (str): The name of the Multus network.
         - multus_ip (str): The static IP address to assign.
         - gateway (str): The gateway IP address.
         - bridge_name (str, optional): The bridge interface name (defaults to "br0").
    - namespace (str): Kubernetes namespace.
    
    Returns:
    - bool: True if all NetworkAttachmentDefinitions were created successfully, False otherwise.
    """
    success = True
    for cfg in multus_configs:
        network_name = cfg.get("network_name")
        multus_ip = cfg.get("multus_ip")
        gateway = cfg.get("gateway")
        bridge_name = cfg.get("bridge_name", "br0")
        
        if not network_name or not multus_ip or not gateway:
            print(f"[ERROR] Missing required Multus configuration in {cfg}.")
            success = False
            continue

        net_attach_def_manifest = {
            "apiVersion": "k8s.cni.cncf.io/v1",
            "kind": "NetworkAttachmentDefinition",
            "metadata": {
                "name": network_name,
                "namespace": namespace
            },
            "spec": {
                "config": f'{{ "cniVersion": "0.3.0", "type": "bridge", "bridge": "{bridge_name}", "ipam": {{ "type": "static", "addresses": [ {{ "address": "{multus_ip}", "gateway": "{gateway}" }} ] }} }}'
            }
        }
        try:
            api_instance.create_namespaced_custom_object(
                group="k8s.cni.cncf.io",
                version="v1",
                namespace=namespace,
                plural="network-attachment-definitions",
                body=net_attach_def_manifest
            )
            print(f"[SUCCESS] NetworkAttachmentDefinition '{network_name}' created with IP '{multus_ip}' and gateway '{gateway}'.")
        except Exception as e:
            print(f"[ERROR] Failed to create NetworkAttachmentDefinition '{network_name}': {e}")
            success = False
    return success


def delete_network_attachment_definition(network_name, namespace="default"):
    """
    Deletes a NetworkAttachmentDefinition by name.

    Args:
      - network_name (str): The name of the NetworkAttachmentDefinition to delete.
      - namespace (str): The Kubernetes namespace from which to delete the definition.

    Returns:
      - bool: True if the deletion was successful (or if it doesn't exist), False otherwise.
    """
    try:
        api_instance.delete_namespaced_custom_object(
            group="k8s.cni.cncf.io",
            version="v1",
            namespace=namespace,
            plural="network-attachment-definitions",
            name=network_name
        )
        print(f"[SUCCESS] NetworkAttachmentDefinition '{network_name}' deleted successfully.")
        return True
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"[INFO] NetworkAttachmentDefinition '{network_name}' not found. It may have already been deleted.")
            return True
        else:
            print(f"[ERROR] Failed to delete NetworkAttachmentDefinition '{network_name}': {e}")
            return False


def create_nodeport_service(selector_key, service_name, namespace, target_port=6443):
    """
    Creates a Kubernetes NodePort service with a selector that matches {selector_key: service_name}.
    The service will target the given port on the pods and a nodePort will be allocated dynamically.
    
    Args:
      - selector_key (str): The label key used in the service selector.
      - service_name (str): The name of the service.
      - namespace (str): The namespace where the service is created.
      - target_port (int, optional): The target port on the pods (default is 6443).
      
    Returns:
      - int: The allocated nodePort if the service was created successfully.
      - bool: False if the service creation failed.
    """
    # Load Kubernetes configuration (adjust if running inside a cluster)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    service_body = client.V1Service(
        metadata=client.V1ObjectMeta(
            name=service_name,
            namespace=namespace
        ),
        spec=client.V1ServiceSpec(
            type="NodePort",
            selector={"special": selector_key},
            ports=[client.V1ServicePort(
                port=target_port,
                target_port=target_port,
                protocol="TCP"
                # node_port is intentionally not set to allow dynamic allocation.
            )]
        )
    )
    
    try:
        created_service = v1.create_namespaced_service(namespace=namespace, body=service_body)
        node_port = created_service.spec.ports[0].node_port
        print(f"[SUCCESS] Service '{service_name}' created with nodePort: {node_port}")
        return node_port
    except client.exceptions.ApiException as e:
        print(f"[ERROR] Failed to create service '{service_name}': {e}")
        return False


def create_admin_ssh_pod(namespace, pod_name="admin-ssh-pod", image="ictu/sshpass", command=["sleep", "3600"]):
    """
    Creates an admin pod using the 'dockette/ssh' image, which has an SSH client installed.
    The pod will run a long-lived command (default is "sleep 3600") so that it remains available for exec.
    
    Args:
      - namespace (str): The namespace to create the pod.
      - pod_name (str): The name of the pod.
      - image (str): The container image (default: "dockette/ssh").
      - command (list): The command the container runs (default: ["sleep", "3600"]).
    
    Returns:
      - str: The name of the created pod if successful, None otherwise.
    """
    config.load_kube_config()
    v1 = client.CoreV1Api()
    
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": namespace
        },
        "spec": {
            "containers": [
                {
                    "name": "admin",
                    "image": image,
                    "command": command
                }
            ],
            "restartPolicy": "Never"
        }
    }
    
    try:
        v1.create_namespaced_pod(namespace=namespace, body=pod_manifest)
        print(f"[SUCCESS] Admin pod '{pod_name}' created in namespace '{namespace}'.")
    except client.exceptions.ApiException as e:
        print(f"[ERROR] Failed to create admin pod '{pod_name}': {e}")
        return None

    # Wait until the pod is Running
    for i in range(30):
        pod = v1.read_namespaced_pod(pod_name, namespace)
        if pod.status.phase == "Running":
            print(f"[INFO] Admin pod '{pod_name}' is running.")
            return pod_name
        time.sleep(2)
    print(f"[ERROR] Admin pod '{pod_name}' did not reach Running state.")
    return None


def delete_admin_ssh_pod(pod_name, namespace):
    """
    Deletes the specified admin pod from the given namespace.
    
    Args:
      - pod_name (str): The name of the admin pod.
      - namespace (str): The namespace where the pod is located.
      
    Returns:
      - True if deletion was successful; False otherwise.
    """
    try:
        config.load_kube_config()
        v1 = client.CoreV1Api()
        v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
        print("[SUCCESS] Admin pod '{}' deleted successfully.".format(pod_name))
        return True
    except Exception as e:
        print("[ERROR] Failed to delete admin pod '{}': {}".format(pod_name, e))
        return False

def exec_command_in_admin_pod(pod_name, namespace, command):
    """
    Executes a command inside the given pod using the Kubernetes exec API.
    
    Args:
      - pod_name (str): The name of the pod.
      - namespace (str): The pod's namespace.
      - command (list): The command to execute (e.g., ["sh", "-c", "ssh user@<ip> 'cat /etc/k3s/k3s.yaml'"]).
    
    Returns:
      - str: The output from the command, or None if it fails.
    """
    config.load_kube_config()
    v1 = client.CoreV1Api()
    try:
        output = stream(v1.connect_get_namespaced_pod_exec,
                        pod_name,
                        namespace,
                        command=command,
                        stderr=True, stdin=False,
                        stdout=True, tty=False)
        return output
    except Exception as e:
        print(f"[ERROR] Exec command failed in pod '{pod_name}': {e}")
        return None


def create_kubevirt_vmi(
    vmi_name,
    dv_name,
    namespace="default",
    cpu_cores=2,
    ram="2048M",
    labeling_enabled=False,
    label="slices-gr",
    multus_interfaces=None,
    registration_command="",
    dns_config_enabled = True,
    dns_config = ["172.28.2.89"]
):
    """
    Creates a KubeVirt VirtualMachineInstance (VMI) that uses a DataVolume for its disk.
    
    Args:
      - vmi_name (str): Name of the VirtualMachineInstance.
      - dv_name (str): Name of the DataVolume to attach as the root disk.
      - namespace (str): Kubernetes namespace.
      - cpu_cores (int): Number of CPU cores.
      - ram (str): Amount of RAM (e.g., "2048M", "4Gi").
      - labeling_enabled (bool): Whether to apply the label and nodeSelector.
      - label (str): The key for the VMI label.
      - multus_interfaces (list or None): A list of dictionaries containing Multus configuration.
           Each dictionary should include:
             - network_name (str)
             - multus_ip (str)
             - gateway (str)
             - (optional) bridge_name (str, defaults to "br0")
           If None or an empty list, no Multus interfaces are attached.
      - registration_command (str): The command to register the node; this will be appended to the cloud-init.
      - dns_config_enabled (bool): Whether to enable custom DNS configuration.
      - dns_config (list): List of IP addresses (as strings) to use as nameservers.
    
    Returns:
      - bool: True if the VMI was created successfully, False otherwise.
    """

    base_cloud_init = """#cloud-config
password: ubuntu
ssh_pwauth: True
chpasswd: { expire: False }
runcmd:
  - |
    # Loop over all network interfaces except lo
    for iface in $(ls /sys/class/net | grep -v lo); do
      # Check if the interface already has an IP address
      if ! ip addr show "$iface" | grep -q "inet "; then
         echo "Bringing up interface $iface with DHCP..."
         dhclient "$iface"
      else
         echo "Interface $iface already has an IP address."
      fi
    done
"""
    # Append the registration command (if provided) to the cloud-init configuration
    if registration_command:
        base_cloud_init += f"\n  - |\n    {registration_command}"
        # print(base_cloud_init)    

    # Define metadata and labels
    metadata = {
        "name": vmi_name,
        "namespace": namespace,
        "labels": {
            "kubevirt.io/vm": vmi_name,
            "special": f"{vmi_name}-key"
        }
    }
    if labeling_enabled:
        metadata["labels"][label] = "true"

    # Define the base VMI manifest
    vmi_manifest = {
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachineInstance",
        "metadata": metadata,
        "spec": {
            "hostname": vmi_name, 
            "domain": {
                "cpu": {"cores": cpu_cores},
                "resources": {"requests": {"memory": ram}},
                "devices": {
                    "disks": [
                        {"name": "datavolumedisk1", "disk": {"bus": "virtio"}},
                        {"name": "cloudinitdisk", "disk": {"bus": "virtio"}}
                    ],
                    "interfaces": [
                        {"name": "default", "bridge": {}}
                    ]
                }
            },
            "networks": [
                {"name": "default", "pod": {}}
            ],
            "volumes": [
                {"name": "datavolumedisk1", "dataVolume": {"name": dv_name}},
                {"name": "cloudinitdisk", "cloudInitNoCloud": {"userData": base_cloud_init}}
            ]
        }
    }

    # Apply nodeSelector if labeling is enabled
    if labeling_enabled:
        vmi_manifest["spec"]["nodeSelector"] = {label: "true"}

    # Process Multus interfaces if provided
    if multus_interfaces and isinstance(multus_interfaces, list) and len(multus_interfaces) > 0:
        # First, create the corresponding NetworkAttachmentDefinitions
        if not create_network_attachment_definitions(multus_interfaces, namespace):
            print("[ERROR] One or more NetworkAttachmentDefinitions failed to create.")
            return False

        # For each Multus interface configuration, add an interface and network entry
        for iface in multus_interfaces:
            network_name = iface.get("network_name")
            if not network_name:
                continue  # Skip if not defined
            # Append an interface with a bridge for this Multus network
            vmi_manifest["spec"]["domain"]["devices"]["interfaces"].append(
                {"name": network_name, "bridge": {}}
            )
            # Append a network entry for this Multus interface
            vmi_manifest["spec"]["networks"].append(
                {"name": network_name, "multus": {"networkName": network_name}}
            )

    # Add DNS configuration if enabled
    if dns_config_enabled:
        if dns_config and isinstance(dns_config, list) and len(dns_config) > 0:
            # Iterate over the provided nameservers and add them to the dnsConfig
            nameservers = []
            for ns in dns_config:
                nameservers.append(ns)
            vmi_manifest["spec"]["dnsConfig"] = {"nameservers": nameservers}
        else:
            print("[WARNING] DNS configuration enabled but no valid nameservers provided.")

    # Create the Virtual Machine Instance (VMI)
    try:
        api_instance.create_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachineinstances",
            body=vmi_manifest
        )
        print(f"[SUCCESS] VMI '{vmi_name}' created using DataVolume '{dv_name}'")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create VMI '{vmi_name}': {e}")
        return False



def delete_kubevirt_vmi(vmi_name, namespace="default"):
    """
    Deletes a KubeVirt VirtualMachineInstance (VMI).
    
    Args:
    - vmi_name (str): Name of the VirtualMachineInstance to delete.
    - namespace (str): Kubernetes namespace where the VMI is located.
    
    Returns:
    - bool: True if the VMI was deleted successfully, False otherwise.
    """

    try:
        api_instance.delete_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachineinstances",
            name=vmi_name
        )
        print(f"[SUCCESS] VMI '{vmi_name}' deleted successfully.")
        return True
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"[INFO] VMI '{vmi_name}' not found. It may have already been deleted.")
            return False
        print(f"[ERROR] Failed to delete VMI '{vmi_name}': {e}")
        return False


def get_vmi_state(vmi_name, namespace="default"):
    """
    Retrieves the state (phase) of a VirtualMachineInstance (VMI).
    
    Args:
      - vmi_name (str): The name of the VirtualMachineInstance.
      - namespace (str): The Kubernetes namespace in which the VMI is located.
      
    Returns:
      - str: The state (phase) of the VMI (e.g., "Running", "Pending", "Succeeded", etc.),
             or None if the VMI is not found or an error occurs.
    """
    try:
        vmi = api_instance.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachineinstances",
            name=vmi_name
        )
        # Retrieve the state from the status field
        state = vmi.get("status", {}).get("phase", "Unknown")
        print(f"[INFO] VMI '{vmi_name}' in namespace '{namespace}' is in state: {state}")
        return state
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"[INFO] VMI '{vmi_name}' not found in namespace '{namespace}'.")
        else:
            print(f"[ERROR] Failed to get VMI '{vmi_name}': {e}")
        return None

def get_vmi_guest_ip(vmi_name, namespace="default"):
    """
    Retrieves the guest IP address of a VirtualMachineInstance (VMI) from its status.
    
    This function looks for an "interfaces" field in the VMI status (populated by the guest agent)
    and returns the IP address from the first interface.
    
    Args:
      - vmi_name (str): The name of the VMI.
      - namespace (str): The namespace where the VMI is deployed.
    
    Returns:
      - str: The guest IP address if available, or None otherwise.
    """
    try:
        config.load_kube_config()
    except Exception as e:
        print(f"[ERROR] Failed to load kube config: {e}")
        return None

    custom_api = client.CustomObjectsApi()
    try:
        vmi_obj = custom_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachineinstances",
            name=vmi_name
        )
    except Exception as e:
        print(f"[ERROR] Failed to get VMI '{vmi_name}' in namespace '{namespace}': {e}")
        return None

    interfaces = vmi_obj.get("status", {}).get("interfaces", [])
    if interfaces:
        # Return the IP address from the first interface.
        guest_ip = interfaces[0].get("ipAddress")
        if guest_ip:
            print(f"[INFO] VMI '{vmi_name}' guest IP address is: {guest_ip}")
            return guest_ip
        else:
            print(f"[ERROR] VMI '{vmi_name}' status interface does not contain an ipAddress.")
            return None
    else:
        print(f"[ERROR] No interfaces found in VMI '{vmi_name}' status. Ensure that the guest agent is running.")
        return None

def get_vmi_host_internal_ip(vmi_name, namespace="default"):
    """
    Retrieves the internal IP address of the Kubernetes node hosting the specified VMI.
    
    Args:
      - vmi_name (str): Name of the VirtualMachineInstance.
      - namespace (str): Namespace in which the VMI is running.
      
    Returns:
      - str: The internal IP address of the node hosting the VMI, or None if not found.
    """
    # Load the Kubernetes configuration (if not already loaded)
    config.load_kube_config()
    
    # Use the KubeVirt CustomObjectsApi to get the VMI object
    custom_api = client.CustomObjectsApi()
    try:
        vmi_obj = custom_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachineinstances",
            name=vmi_name
        )
    except Exception as e:
        print(f"[ERROR] Failed to get VMI '{vmi_name}' in namespace '{namespace}': {e}")
        return None

    # Check if the VMI's status has the nodeName field
    node_name = vmi_obj.get("status", {}).get("nodeName")
    if not node_name:
        print(f"[ERROR] VMI '{vmi_name}' does not have a nodeName in its status.")
        return None

    # Use CoreV1Api to retrieve the Node object
    v1 = client.CoreV1Api()
    try:
        node_obj = v1.read_node(node_name)
    except Exception as e:
        print(f"[ERROR] Failed to retrieve Node '{node_name}': {e}")
        return None

    # Iterate through the node's addresses to find the InternalIP
    for address in node_obj.status.addresses:
        if address.type == "InternalIP":
            return address.address

    print(f"[ERROR] Node '{node_name}' does not have an InternalIP address.")
    return None

def get_data_volume(dv_name, namespace):
    """
    Retrieves the details of a DataVolume (DV) in the specified namespace.

    Args:
      - dv_name (str): The name of the DataVolume.
      - namespace (str): The Kubernetes namespace where the DV is located.

    Returns:
      - dict: The DataVolume object if found, otherwise None.
    """
    try:
        dv = api_instance.get_namespaced_custom_object(
            group="cdi.kubevirt.io",
            version="v1beta1",
            namespace=namespace,
            plural="datavolumes",
            name=dv_name
        )
        return dv
    except Exception as e:
        print(f"[ERROR] Unable to retrieve DataVolume '{dv_name}' in namespace '{namespace}': {e}")
        return None


def wait_for_dv_ready(dv_name, namespace, timeout=300, interval=3):
    """
    Polls until the DataVolume's status is 'WaitForFirstConsumer' or 'Succeeded'.
    
    Args:
      - dv_name (str): Name of the DataVolume.
      - namespace (str): The namespace of the DataVolume.
      - timeout (int): Maximum time in seconds to wait.
      - interval (int): Polling interval in seconds.
    
    Returns:
      - bool: True if DV reached a ready state within timeout, False otherwise.
    """
    start = time.time()
    while time.time() - start < timeout:
        dv = get_data_volume(dv_name, namespace)  # Assume you have a helper that fetches the DV object.
        status = dv.get("status", {}).get("phase", "") if dv else ""
        if status in ["WaitForFirstConsumer", "Succeeded"]:
            print(f"[INFO] DataVolume '{dv_name}' is ready with status '{status}'.")
            return True
        print(f"[INFO] DataVolume '{dv_name}' status is '{status}'. Waiting...")
        time.sleep(interval)
    print(f"[ERROR] Timeout waiting for DataVolume '{dv_name}' to be ready.")
    return False

def extract_kubeconfig_from_vmi(vmi_name, namespace, cluster_type="rke2", admin_pod_name="admin-ssh-pod", ssh_password="ubuntu"):
    """
    Extracts the kubeconfig file from a VMI by using an admin pod to SSH into the VMI.
    
    This function:
      - Determines the expected kubeconfig file path based on the cluster type.
      - Retrieves the guest IP of the VMI (via get_vmi_guest_ip).
      - Builds an SSH command using sshpass to supply the password.
      - Uses exec_command_in_admin_pod to execute that command.
    
    Args:
      - vmi_name (str): The name of the VMI (typically the master).
      - namespace (str): The namespace where the VMI is deployed.
      - cluster_type (str): Either "rke2" or "k3s"; determines the kubeconfig path.
      - admin_pod_name (str): The name of the admin pod.
      - ssh_password (str): The SSH password for the user (default is "ubuntu").
    
    Returns:
      - str: The kubeconfig content if successful, or None if it fails.
    """
    if cluster_type.lower() == "rke2":
        kubeconfig_path = "/etc/rancher/rke2/rke2.yaml"
    elif cluster_type.lower() == "k3s":
        kubeconfig_path = "/etc/rancher/k3s/k3s.yaml"
    else:
        print("[ERROR] Unsupported cluster type: {0}".format(cluster_type))
        return None

    # Retrieve the guest IP (assumes get_vmi_guest_ip is defined)
    guest_ip = get_vmi_guest_ip(vmi_name, namespace)
    if not guest_ip:
        print("[ERROR] Could not retrieve guest IP for VMI '{0}'".format(vmi_name))
        return None

    # Build the SSH command using sshpass. This command will supply the password.
    ssh_cmd = [
        "sh", "-c", 
        "sshpass -p \"{pwd}\" ssh -o StrictHostKeyChecking=no ubuntu@{ip} 'sudo cat {path}'".format(
            pwd=ssh_password, ip=guest_ip, path=kubeconfig_path
        )
    ]
    print("[INFO] Executing SSH command from admin pod: {0}".format(ssh_cmd))
    
    kubeconfig = exec_command_in_admin_pod(admin_pod_name, namespace, ssh_cmd)
    if not kubeconfig:
        print("[ERROR] Failed to extract kubeconfig via SSH.")
        return None

    return kubeconfig

def update_kubeconfig_server(kubeconfig_str, host_ip, node_port):
    """
    Updates the server field in a kubeconfig file.

    This function parses the given kubeconfig YAML string, replaces the server URL
    (which is expected to be something like "https://127.0.0.1:6443") with a new URL
    constructed from the provided host_ip and node_port, and then returns the updated YAML string.

    Args:
      - kubeconfig_str (str): The original kubeconfig content as a YAML string.
      - host_ip (str): The new host IP address.
      - node_port (int or str): The new node port.
      
    Returns:
      - str: The updated kubeconfig YAML string, or None if an error occurs.
    """
    try:
        config_data = yaml.safe_load(kubeconfig_str)
    except Exception as e:
        print("Error parsing kubeconfig YAML: {}".format(e))
        return None

    if "clusters" not in config_data:
        print("No clusters found in kubeconfig.")
        return None

    # Update the server field in each cluster entry
    for cluster_entry in config_data["clusters"]:
        if "cluster" in cluster_entry and "server" in cluster_entry["cluster"]:
            new_server = "https://" + host_ip + ":" + str(node_port)
            cluster_entry["cluster"]["server"] = new_server

    try:
        new_kubeconfig_str = yaml.safe_dump(config_data, default_flow_style=False)
        return new_kubeconfig_str
    except Exception as e:
        print("Error dumping updated kubeconfig YAML: {}".format(e))
        return None

def clean_kubeconfig(kubeconfig_str):
    """
    Removes the first line if it starts with "Warning:" from the kubeconfig string.
    
    Args:
      - kubeconfig_str (str): The original kubeconfig string.
      
    Returns:
      - str: The cleaned kubeconfig string.
    """
    lines = kubeconfig_str.splitlines()
    if lines and lines[0].startswith("Warning:"):
        return "\n".join(lines[1:])
    return kubeconfig_str

async def create_all_dvs(vm_defs, namespace):
    """
    Launch DV creation tasks for all VMs concurrently.
    
    Args:
      - vm_defs (list): List of VM definitions. Each must have a "vmi_name", "disk_size", and "disk_image".
      - namespace (str): Namespace for the DVs.
    
    Returns:
      - dict: Mapping from vmi_name to its DV name (or None if creation failed).
    """
    tasks = {}
    for vm in vm_defs:
        vmi_name = vm.get("vmi_name")
        if not vmi_name:
            continue
        disk_size = vm.get("disk_size", 20)
        storage_size = f"{disk_size}Gi" if isinstance(disk_size, (int, float)) else disk_size
        disk_image = vm.get("disk_image", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
        dv_name = f"{vmi_name}-dv"
        tasks[vmi_name] = asyncio.create_task(asyncio.to_thread(create_data_volume, dv_name, disk_image, storage_size, namespace=namespace))
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    # Build mapping: if task succeeded (returns a truthy value), map vmi_name -> dv_name; otherwise None.
    dv_mapping = {vmi: (f"{vmi}-dv" if result else None) for vmi, result in zip(tasks.keys(), results)}
    return dv_mapping

def run_registration_command_on_vm(vmi_name, namespace, password, registration_command, admin_pod_name="admin-ssh-pod", ssh_username="ubuntu"):
    """
    Uses SSH (via sshpass) from an admin pod to connect to the specified VMI and execute the registration command.
    
    Args:
      - vmi_name (str): The name of the VirtualMachineInstance.
      - namespace (str): The Kubernetes namespace where the VMI is deployed.
      - password (str): The SSH password for the VM user.
      - registration_command (str): The command to register the VM.
      - admin_pod_name (str): The name of the admin pod that will be used to exec the SSH command.
      - ssh_username (str): The SSH username (default is "ubuntu").
    
    Returns:
      - str: The output of the registration command if successful.
      - None: if an error occurs.
    """
    # Determine the SSH command to run.
    # We assume that the registration command should be executed on the guest via SSH.
    # First, retrieve the guest IP of the VMI.
    guest_ip = get_vmi_guest_ip(vmi_name, namespace)
    if not guest_ip:
        print("Failed to retrieve guest IP for VMI '{}'.".format(vmi_name))
        return None

    # Build the SSH command using sshpass to supply the password.
    # This command will SSH into the guest (using its IP) as the specified user
    # and then execute the registration command.
    ssh_cmd = [
        "sh", "-c",
        "sshpass -p \"{pwd}\" ssh -o StrictHostKeyChecking=no {user}@{ip} 'sudo {reg_cmd}'".format(
            pwd=password, user=ssh_username, ip=guest_ip, reg_cmd=registration_command
        )
    ]
    print("Executing SSH command from admin pod: {}".format(ssh_cmd))
    
    # Execute the command in the admin pod using the exec helper.
    output = exec_command_in_admin_pod(admin_pod_name, namespace, ssh_cmd)
    if output is None:
        print("Failed to execute registration command on VMI '{}'.".format(vmi_name))
    return output


async def wait_for_vm_registration(vmi_name, internal_cluster_id, namespace, poll_interval=5, timeout=300):
    """
    Waits until the VMI (identified by vmi_name) appears in the cluster nodes.
    
    Args:
      - vmi_name (str): The VMI name.
      - internal_cluster_id (str): The internal ID of the cluster.
      - namespace (str): The namespace.
      - poll_interval (int): Seconds between polls.
      - timeout (int): Maximum wait time in seconds.
    
    Returns:
      - True if the VMI registers within timeout; False otherwise.
    """
    start = time.time()
    while time.time() - start < timeout:
        nodes = await asyncio.to_thread(list_cluster_nodes_v3, internal_cluster_id)
        if any(vmi_name in (node.get("hostname", "") + node.get("name", "")) for node in nodes):
            print("[INFO] VMI '{}' is registered in the cluster.".format(vmi_name))
            return True
        await asyncio.sleep(poll_interval)
    print("[ERROR] Timeout waiting for VMI '{}' to register.".format(vmi_name))
    return False


async def create_kubevirt_cluster_async(cluster_name, namespace, cluster_type="rke2",
                                          kubernetes_version="v1.31.5+rke2r1", cni="flannel",
                                          vm_definitions=None):
    """
    Asynchronously creates a Rancher cluster and deploys VMIs on KubeVirt.
    
    Workflow:
      1. Create the Rancher cluster sequentially, using new CIDRs to avoid overlapping
         with the parent cluster's networking.
      2. Retrieve the internal cluster name and poll until a registration command is available.
      3. Split VM definitions into master and worker lists.
      4. Create DataVolumes for all VMs concurrently.
      5. Provision all VMIs concurrently (master and worker VMs in parallel, each waiting only on its own DV).
      6. Wait until the master VMI is Running and registered.
      7. Create the admin pod concurrently with the VMs.
      8. Once the master is registered, run admin operations concurrently:
            a) SSH into the master to extract and update the kubeconfig.
            b) SSH concurrently into each worker to run the registration command.
      9. Deleting the admin pod and returning the updated kubeconfig.
    
    Returns:
      - The updated kubeconfig (str) if all operations succeed.
      - False if any step fails.
    """
    rancher_namespace = "default"  # Rancher provisioning happens in "default"

    # Create the target namespace if needed.
    if namespace != "default":
        ns = await asyncio.to_thread(create_namespace, namespace)
        if not ns:
            print("[ERROR] Failed to create namespace '{}'.".format(namespace))
            return False

    new_pod_cidr, new_service_cidr = get_new_cidrs()
    if not new_pod_cidr or not new_service_cidr:
        print("[ERROR] Failed to compute new CIDRs.")
        return False
    print("[INFO] Proposed new pod CIDR: {}, new service CIDR: {}".format(new_pod_cidr, new_service_cidr))
    
    # Step 1: Create the cluster sequentially, passing the new CIDRs and the TLS-SANs from admin k8s cluster (in order to access the cluster from an endpoint)
    ips_admin_k8s_nodes = get_k8s_node_ips()
    if not ips_admin_k8s_nodes:
        print("[ERROR] Failed to get master IP and certificate")
        return False
    cluster_response = await asyncio.to_thread(create_custom_cluster,
                                                 cluster_name,
                                                 cluster_type,
                                                 kubernetes_version,
                                                 rancher_namespace,
                                                 cni,
                                                 pod_cidr=new_pod_cidr,
                                                 service_cidr=new_service_cidr,
                                                 tls_san=ips_admin_k8s_nodes)
    if not cluster_response or cluster_response == 409:
        print("[ERROR] Failed to create cluster '{}' in namespace '{}'.".format(cluster_name, rancher_namespace))
        return False
    print("[INFO] Cluster '{}' created successfully in namespace '{}'.".format(cluster_name, rancher_namespace))

    # Retrieve internal cluster name and poll for registration command.
    internal_cluster_id = await wait_for_internal_cluster_name(rancher_namespace, cluster_name)
    if not internal_cluster_id:
        print("[ERROR] Unable to obtain internal cluster name after waiting.")
        return False
    reg_command_base = await asyncio.to_thread(wait_for_registration_command, internal_cluster_id)

    if not reg_command_base:
        print("[ERROR] Unable to obtain registration command within the timeout period.")
        return False
    worker_reg_cmd = reg_command_base + " --worker"

    # Split VM definitions.
    if vm_definitions is None:
        vm_definitions = []
    master_vms = [vm for vm in vm_definitions if "control plane" in vm.get("roles", [])]
    worker_vms = [vm for vm in vm_definitions if ("worker" in vm.get("roles", [])) and ("control plane" not in vm.get("roles", []))]
    all_vms = master_vms + worker_vms

    # Create DataVolumes for all VMs concurrently.
    dv_mapping = await create_all_dvs(all_vms, namespace)
    print("[INFO] DataVolume creation mapping:", dv_mapping)

    # Schedule admin pod creation concurrently, without blocking.
    admin_pod_task = asyncio.create_task(
        asyncio.to_thread(create_admin_ssh_pod, namespace, pod_name="admin-ssh-pod", image="ictu/sshpass")
    )

    # Provision all VMIs concurrently.
    async def provision_vm(vm):
        vmi_name = vm.get("vmi_name")
        if not vmi_name:
            print("[ERROR] VM definition missing 'vmi_name'.")
            return False
        dv_name_vm = dv_mapping.get(vmi_name)
        if not dv_name_vm:
            print("[ERROR] DataVolume for '{}' not found.".format(vmi_name))
            return False
        # Wait for this DV to be ready (each VM waits for its own DV).
        ready = await asyncio.to_thread(wait_for_dv_ready, dv_name_vm, namespace)
        if not ready:
            print("[ERROR] DV '{}' for '{}' is not ready.".format(dv_name_vm, vmi_name))
            return False
        # Build final registration command only for master VMs.
        if "control plane" in vm.get("roles", []):
            final_reg_cmd = reg_command_base + " --etcd --controlplane"
            if "worker" in vm.get("roles", []):
                final_reg_cmd += " --worker"
        else:
            final_reg_cmd = ""
        result = await asyncio.to_thread(
            create_kubevirt_vmi,
            vmi_name,
            dv_name_vm,
            namespace,
            vm.get("cpu_cores", 2),
            vm.get("ram", "2048M"),
            vm.get("labeling_enabled", False),
            vm.get("label", "slices-gr"),
            vm.get("multus_interfaces", None),
            final_reg_cmd,
            vm.get("dns_config_enabled", True),
            vm.get("dns_config", ["172.28.2.89"])
        )
        return result

    vm_tasks = [asyncio.create_task(provision_vm(vm)) for vm in all_vms]
    vm_results = await asyncio.gather(*vm_tasks, return_exceptions=True)
    for res in vm_results:
        if isinstance(res, Exception) or not res:
            print("[ERROR] One or more VMIs failed to provision.")
            return False

    # Wait until the master VMI is Running and registered.
    master_vmi_name = master_vms[0].get("vmi_name")
    state = await asyncio.to_thread(get_vmi_state, master_vmi_name, namespace)
    while state != "Running" and state is not None:
        print("[INFO] Master VMI '{}' state: {}. Retrying in 3 seconds...".format(master_vmi_name, state))
        await asyncio.sleep(3)
        state = await asyncio.to_thread(get_vmi_state, master_vmi_name, namespace)
    master_registered = False
    master_registered = await wait_for_vm_registration(master_vmi_name, internal_cluster_id, namespace)
    if not master_registered:
        print("[ERROR] Primary master VMI '{}' failed to register.".format(master_vmi_name))
        return False
    
    print("[INFO] Master VMI '{}' is successfully registered in the cluster.".format(master_vmi_name))


    # Run admin operations concurrently.
    # Admin master ops: SSH into master to extract kubeconfig, create NodePort service on port 6443,
    #     get the host IP, update the kubeconfig, and return it.
    async def admin_master_ops():
        master_kubeconfig = extract_kubeconfig_from_vmi(master_vmi_name, namespace, cluster_type, admin_pod_name="admin-ssh-pod")
        if not master_kubeconfig:
            print("[ERROR] Failed to extract kubeconfig from master.")
            return None
        node_port = await asyncio.to_thread(create_nodeport_service, "cluster-master-key", "cluster-master-svc", namespace, 6443)
        if not node_port:
            print("[ERROR] Failed to create NodePort service for master.")
            return None
        host_ip = await asyncio.to_thread(get_vmi_host_internal_ip, master_vmi_name, namespace)
        if not host_ip:
            print("[ERROR] Failed to get host IP for master.")
            return None
        updated_kubeconfig = update_kubeconfig_server(master_kubeconfig, host_ip, node_port)
        if updated_kubeconfig:
            updated_kubeconfig = clean_kubeconfig(updated_kubeconfig)
        return updated_kubeconfig

    await admin_pod_task
    admin_master_task = asyncio.create_task(admin_master_ops())

    # Admin worker ops: For each worker, SSH into it and run the registration command concurrently.
    async def admin_worker_op(vm):
        return await asyncio.to_thread(run_registration_command_on_vm,
                                       vm.get("vmi_name"), namespace, "ubuntu",
                                       worker_reg_cmd, "admin-ssh-pod")
    admin_worker_tasks = [asyncio.create_task(admin_worker_op(vm)) for vm in worker_vms]
    admin_worker_results = await asyncio.gather(*admin_worker_tasks, return_exceptions=True)
    for res in admin_worker_results:
        if isinstance(res, Exception) or not res:
            print("[ERROR] One or more worker registration commands failed.")
            # Optionally, decide whether to fail overall here.
    
    updated_kubeconfig = await admin_master_task
    if not updated_kubeconfig:
        print("[ERROR] Admin master operations failed.")
        return False

    # Ensure every Worker/Secondary Master VM is registered in the cluster.
    other_vms = master_vms[1:] + worker_vms
    registration_tasks = [asyncio.create_task(wait_for_vm_registration(vm.get("vmi_name"), internal_cluster_id, namespace))
                          for vm in other_vms]
    registration_results = await asyncio.gather(*registration_tasks, return_exceptions=True)
    for res in registration_results:
        if isinstance(res, Exception) or not res:
            print("[ERROR] One or more secondary VMs failed to register.")
            return False

    # Wait for master admin operations to complete.
    updated_kubeconfig = await admin_master_task
    if not updated_kubeconfig:
        print("[ERROR] Admin master operations failed.")
        return False

    # Schedule admin pod deletion in the background
    asyncio.create_task(asyncio.to_thread(delete_admin_ssh_pod, "admin-ssh-pod", namespace))

    print("[SUCCESS] All operations completed successfully. Returning updated kubeconfig.")
    
    return updated_kubeconfig
    
def create_kubevirt_cluster(cluster_name, namespace, cluster_type="rke2", kubernetes_version="v1.31.5+rke2r1", cni="flannel", vm_definitions=None):
    return asyncio.run(create_kubevirt_cluster_async(cluster_name, namespace, cluster_type, kubernetes_version, cni, vm_definitions))


if __name__ == "__main__":

    #delete_namespace("testing-k3s")
    #delete_provisioned_cluster("final-test", namespace="default")


    cluster_name = "final-test"
    namespace = "testing-k3s"
    cluster_type = "k3s"
    kubernetes_version = "v1.31.5+k3s1"
    cni = "flannel"

    multus_configs1 = [
        {
            "network_name": "net1",
            "multus_ip": "192.168.18.203/24",
            "gateway": "192.168.18.1",
            "bridge_name": "br0"
        }
    ]
    multus_configs2 = [
        {
            "network_name": "net2",
            "multus_ip": "192.168.18.204/24",
            "gateway": "192.168.18.1",
            "bridge_name": "br0"
        }
    ]
    vm_definitions = [
        {
            "vmi_name": "cluster-master",
            "disk_size": 15,
            "disk_image": "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
            "cpu_cores": 6,
            "ram": "4096M",
            "labeling_enabled": False,
            "label": "control-plane",
            "multus_interfaces": multus_configs1,
            "roles": ["control plane", "worker"] 
        },
        {
            "vmi_name": "worker1",
            "disk_size": 15,
            "disk_image": "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
            "cpu_cores": 6,
            "ram": "4096M",
            "labeling_enabled": False,
            "multus_interfaces": multus_configs2,
            "roles": ["worker"]
        }
    ]

    k8s_config = create_kubevirt_cluster(
        cluster_name=cluster_name,
        namespace=namespace,
        cluster_type=cluster_type,
        kubernetes_version=kubernetes_version,
        cni=cni,
        vm_definitions=vm_definitions
    )

    print(k8s_config)

    # cluster_name = "final-test"
    # namespace = "testing-k3s"
    # cluster_type = "k3s"
    # kubernetes_version = "v1.31.4+k3s1"
    # cni="flannel"

    # multus_configs1 = [
    #     {
    #         "network_name": "net1",
    #         "multus_ip": "192.168.18.203/24",
    #         "gateway": "192.168.18.1",
    #         "bridge_name": "br0"
    #     }
    # ]

    # multus_configs2 = [
    #     {
    #         "network_name": "net2",
    #         "multus_ip": "192.168.18.204/24",
    #         "gateway": "192.168.18.1",
    #         "bridge_name": "br0"
    #     }
    # ]

    # vm_definitions = [
    #     {
    #         "vmi_name": "cluster-master",
    #         "disk_size": 15,
    #         "disk_image": "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
    #         "cpu_cores": 6,
    #         "ram": "4096M",
    #         "labeling_enabled": False,
    #         "label": "control-plane",
    #         "multus_interfaces": multus_configs1,
    #         "roles": ["control plane", "worker"]  # Master has both roles in this example.
    #     },
    #     {
    #         "vmi_name": "worker1",
    #         "disk_size": 15,
    #         "disk_image": "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
    #         "cpu_cores": 6,
    #         "ram": "4096M",
    #         "labeling_enabled": False,
    #         "multus_interfaces": multus_configs2,
    #         "roles": ["worker"]
    #     }
    # ]

    # create_namespace("testing-k3s")

    # create_data_volume(storage_size="12Gi", dv_name="k3s-master-dv", namespace="testing-k3s", image_url="https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
    # create_data_volume(storage_size="10Gi", dv_name="k3s-worker-dv", namespace="testing-k3s", image_url="https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")


    # create_kubevirt_vmi(
    #     vmi_name="cluster-master",
    #     dv_name="k3s-master-dv",
    #     namespace="testing-k3s",
    #     cpu_cores=4,
    #     ram="4096M",
    #     labeling_enabled=False,
    #     label="control-plane",
    #     registration_command="",
    #     dns_config_enabled=True,
    #     dns_config=["172.28.2.89"],
    #     multus_interfaces=multus_configs1
    #     )

    # create_kubevirt_vmi(
    #     vmi_name="worker1",
    #     dv_name="k3s-worker-dv",
    #     namespace="testing-k3s",
    #     cpu_cores=4,
    #     ram="4096M",
    #     labeling_enabled=False,
    #     label="control-plane",
    #     registration_command="",
    #     dns_config_enabled=True,
    #     dns_config=["172.28.2.89"],
    #     multus_interfaces=multus_configs2
    #     )
# create_kubevirt_cluster(
    #     cluster_name=cluster_name,
    #     namespace=namespace,
    #     cluster_type=cluster_type,
    #     cni=cni,
    #     kubernetes_version=kubernetes_version,
    #     vm_definitions=vm_definitions
    # )

    # delete_namespace("testing-k3s")
    # delete_provisioned_cluster("final-test", namespace="default")
     #delete_network_attachment_definition("net1", namespace="default")
     # delete_network_attachment_definition("net2", namespace="default")
     # delete_data_volume("ubuntu-data-volume-new")
     # delete_data_volume("ubuntu-data-volume-new1")
     # delete_data_volume("ubuntu-data-volume1")
     # delete_data_volume("ubuntu-data-volume2")
    #  delete_data_volume("ubuntu-data-volume3")
    
      
      #delete_kubevirt_vmi("k3s-fresh")
    # #delete_data_volume("k3s-fresh")
    # #time.sleep(5)

    #   create_namespace("testing-k3s")

    #   multus_configs1 = [
    #     {
    #         "network_name": "net1",
    #         "multus_ip": "192.168.18.203/24",
    #         "gateway": "192.168.18.1",
    #         "bridge_name": "br0"
    #     }
    # ]

    #   multus_configs2 = [
    #         {
    #             "network_name": "net2",
    #             "multus_ip": "192.168.18.204/24",
    #             "gateway": "192.168.18.1",
    #             "bridge_name": "br0"
    #         }
    #     ]

    # #   create_custom_cluster(cluster_name="k3s-final-test-cluster", cluster_type="k3s", kubernetes_version="v1.31.4+k3s1", namespace="default", cni="flannel")
    #   create_data_volume(storage_size="15Gi", dv_name="k3s-master-dv", namespace="testing-k3s", image_url="https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
    #   create_data_volume(storage_size="15Gi", dv_name="k3s-worker-dv", namespace="testing-k3s", image_url="https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
      
    #   create_kubevirt_vmi(
    #     vmi_name="k3s-master",
    #     dv_name="k3s-master-dv",
    #     namespace="testing-k3s",
    #     cpu_cores=4,
    #     ram="4096M",
    #     labeling_enabled=False,
    #     label="control-plane",
    #     registration_command="",
    #     dns_config_enabled=True,
    #     dns_config=["172.28.2.89"],
    #     multus_interfaces=multus_configs1
    #     )

    #   create_kubevirt_vmi(
    #     vmi_name="k3s-worker",
    #     dv_name="k3s-worker-dv",
    #     namespace="testing-k3s",
    #     cpu_cores=4,
    #     ram="4096M",
    #     labeling_enabled=False,
    #     label="control-plane",
    #     registration_command="",
    #     dns_config_enabled=True,
    #     dns_config=["172.28.2.89"],
    #     multus_interfaces=multus_configs2
    #     )


    # ip = get_vmi_host_internal_ip("k3s-change", "testing-k3s")
    # print(ip)

    # node_port = create_nodeport_service("k3s-change-key","k3s-change-service","testing-k3s")
    # print(node_port)
#     base_cloud_init = """#cloud-config
# password: ubuntu
# ssh_pwauth: True
# chpasswd: { expire: False }
# runcmd:
#   - |
#     # Loop over all network interfaces except lo
#     for iface in $(ls /sys/class/net | grep -v lo); do
#       # Check if the interface already has an IP address
#       if ! ip addr show "$iface" | grep -q "inet "; then
#          echo "Bringing up interface $iface with DHCP..."
#          dhclient "$iface"
#       else
#          echo "Interface $iface already has an IP address."
#       fi
#     done
# """ 
#     vmi_name = "k3s-fresh"
#     hostname_update = f"    echo \"127.0.0.1 {vmi_name}\" | tee -a /etc/hosts\n"    
#     base_cloud_init += hostname_update
#     reg_command  = "curl --insecure -fL https://rancher.sopnode-inria1.theblueprintfactory.org:443/system-agent-install.sh | sudo sh -s - --server https://rancher.sopnode-inria1.theblueprintfactory.org:443 --label 'cattle.io/os=linux' --token lspjdzvbks7sthmnsrfxvrj6xtg5rv2rts8vhdtf57zsfdv89k8lmv --ca-checksum 08dca1de2a55d7c6c804e7a6901d671b1b1221817b0ff383114188be01cccf63 --etcd --controlplane --worker"
#     base_cloud_init += f"\n  - |\n    {reg_command}"
#     print(base_cloud_init)
    # delete_network_attachment_definition("net1")
    # delete_network_attachment_definition("net2")
    # create_data_volume("ubuntu-data-volume-new1", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")

    #create_kubevirt_vmi(vmi_name="ubuntu-vmi-new", dv_name="ubuntu-data-volume-new1", cpu_cores=4, ram="4096M", multus_interfaces=multus_configs)
    #delete_data_volume("ubuntu-data-volume-new")
    # # Replace with your internal cluster ID (e.g., the one you see in the Rancher UI under the cluster details)
    # cluster_id = get_internal_cluster_name("fleet-default", "sopnode-baby")
    # nodes = list_cluster_nodes_v3(cluster_id)
    # if nodes:
    #     print("Nodes in the cluster:")
    #     for node in nodes:
    #         node_id = node.get("id", "Unknown ID")
    #         # Depending on your Rancher version, the node's hostname or name may be stored in different fields
    #         node_name = node.get("hostname", node.get("name", "Unnamed"))
    #         print(f" - {node_name} (ID: {node_id})")
    # else:
    #     print("No nodes were found or an error occurred.")


    # state = get_cluster_state("my-custom-rke2", "fleet-default")
    # print("Cluster state:", state)
    
    # state = get_cluster_state("sopnode-baby", "fleet-default")
    # print("Cluster state:", state)


    #clusters = list_all_clusters_from_provisioning_api()
    #if clusters:
    #    print("List of clusters from the provisioning API:")
    #    for cluster in clusters:
    #        print(f" - {cluster['Cluster Name']} (ID: {cluster['Cluster ID']})")
    #else:
    #    print("No clusters found or an error occurred.")

    #cluster_state = get_cluster_state("sopnode-baby", "fleet-default")
    #print("Cluster state:", cluster_state)
    # multus_configs = [
    #     {
    #         "network_name": "net1",
    #         "multus_ip": "192.168.18.203/24",
    #         "gateway": "192.168.18.1",
    #         "bridge_name": "br0"
    #     },
    #     {
    #         "network_name": "net2",
    #         "multus_ip": "192.168.19.204/24",
    #         "gateway": "192.168.19.1",
    #         "bridge_name": "br0"
    #     }
    # ]

    # delete_kubevirt_vmi(vmi_name="ubuntu-vmi")

    # create_data_volume("ubuntu-data-volume2", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
    # import time
    #time.sleep(10)
    #create_kubevirt_vmi(vmi_name="ubuntu-vmi3", dv_name="ubuntu-data-volume2", cpu_cores=4, ram="4096M", multus_interfaces=multus_configs)

    #vmi_name = "ubuntu-vmi2"  # Replace with your VMI name
    #namespace = "default"     # Replace with the appropriate namespace if needed
    #state = get_vmi_state(vmi_name, namespace)
    #print(f"VMI state: {state}")
    #delete_kubevirt_vmi(vmi_name="ubuntu-vmi2")

    # # Example Multus configuration: define a list of Multus interface dictionaries
    # multus_configs = [
    #     {
    #         "network_name": "net1",
    #         "multus_ip": "192.168.18.203/24",
    #         "gateway": "192.168.18.1",
    #         "bridge_name": "br0"  # Optional: defaults to "br0" if not provided.
    #     }
    # ]

    # # Run the helper function to create the NetworkAttachmentDefinitions
    # if create_network_attachment_definitions(multus_configs, namespace="default"):
    #     print("All NetworkAttachmentDefinitions were created successfully!")
    # else:
    #     print("One or more NetworkAttachmentDefinitions failed to be created.")

    # net_name = "net1"  # Specify the network attachment definition name to delete
    # if delete_network_attachment_definition(net_name, namespace="default"):
    #     print("NetworkAttachmentDefinition deleted successfully.")
    # else:
    #     print("Failed to delete the NetworkAttachmentDefinition.")

    # net_name = "net2"  # Specify the network attachment definition name to delete
    # if delete_network_attachment_definition(net_name, namespace="default"):
    #     print("NetworkAttachmentDefinition deleted successfully.")
    # else:
    #     print("Failed to delete the NetworkAttachmentDefinition.")

    # net_name = "net1"  # Specify the network attachment definition name to delete
    # if delete_network_attachment_definition(net_name, namespace="default"):
    #     print("NetworkAttachmentDefinition deleted successfully.")
    # else:
    #     print("Failed to delete the NetworkAttachmentDefinition.")        

    # delete_kubevirt_vmi(vmi_name="ubuntu-vmi3")
    # delete_data_volume("ubuntu-data-volume3")

    # create_data_volume("ubuntu-data-volume1", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
    # import time
    # time.sleep(10)
    #create_kubevirt_vmi(vmi_name="ubuntu-vmi2", dv_name="ubuntu-data-volume1", cpu_cores=4, ram="4096M")
    #delete_kubevirt_vmi(vmi_name="ubuntu-vmi1")
    #delete_data_volume("ubuntu-data-volume1")

    # mgmt_name = get_internal_cluster_name("fleet-default", "test-final-k3s")
    # command = get_rancher_registration_command(mgmt_name)
    # print("\n[JOIN COMMAND]\n", command, "\n")
    # status = delete_provisioned_cluster("test-final-k3s", namespace="fleet-default")
    # print(status)
    # mgmt_name = get_internal_cluster_name("fleet-default", "ok-custom")
    # print("[INFO] The management cluster name is:", mgmt_name)
    # if mgmt_name:
    #     command = get_rancher_registration_command(mgmt_name)
    #     if command:
    #         print("\n[JOIN COMMAND]\n", command, "\n")
    #     else:
    # print("No command found. Possibly still provisioning or a mismatch.")
