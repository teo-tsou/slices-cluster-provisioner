# Cluster provisioner Library v0.5
# Python Script that automates the Rancher, KubeVirt, and CDI procedures, by taking advantage of the rancher and K8s APIs.   
# Author: Theodoros Tsourdinis
# email:  theodoros.tsourdinis@sorbonne-universite.fr


import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.auth import HTTPBasicAuth
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from kubernetes import config, client
import time
from kubernetes.client.rest import ApiException
import urllib3


# RANCHER Client Credentials
RANCHER_URL = 'define-the-rancher-URL'
RANCHER_ACCESS_KEY = 'define-the-access-key-from-rancher'
RANCHER_SECRET_KEY = 'define-the-rancher-secret-key-from-rancher'
VERIFY_SSL = False

# Load Kubernetes config and client instance
config.load_kube_config()
api_instance = client.CustomObjectsApi()



#######################################
###### K8s Cluster-related Functions ##
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
    kubernetes_version="v1.31.4+rke2r1",
    namespace="fleet-default",
    cni="calico"
):
    """
    Creates a "Custom" cluster in Rancher using the provisioning API (v1).
    Supports both RKE2 and K3s provisioning.
    """
    if not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    provisioning_endpoint = f"{RANCHER_URL.rstrip('/')}/v1/provisioning.cattle.io.clusters"
    headers = {
        "Authorization": f"Bearer {RANCHER_ACCESS_KEY}:{RANCHER_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    # Decide whether we set rkeConfig or k3sConfig
    if cluster_type.lower() == "rke2":
        config_key = "rkeConfig"
        payload = {
            "apiVersion": "provisioning.cattle.io/v1",
            "kind": "Cluster",
            "metadata": {
                "name": cluster_name,
                "namespace": namespace
            },
            "spec": {
                "kubernetesVersion": kubernetes_version,
                config_key: {
                    "cni": cni,
                    "machinePools": []  # Tells Rancher it's a "Custom" RKE2 cluster
                }
            }
        }
    elif cluster_type.lower() == "k3s":
        config_key = "rkeConfig"
        payload = {
            "apiVersion": "provisioning.cattle.io/v1",
            "kind": "Cluster",
            "metadata": {
                "name": cluster_name,
                "namespace": namespace
            },
            "spec": {
                "kubernetesVersion": kubernetes_version,
                config_key: {
                    "cni": "flannel",
                    "machinePools": []
                }
            }
        }
    else:
        raise ValueError("Invalid cluster_type. Must be 'rke2' or 'k3s'.")

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

def wait_for_registration_command(internal_cluster_id, timeout=300, interval=3):
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
    dns_config_enabled = False,
    dns_config = ["8.8.8.8", "1.1.1.1"]
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
    hostname_update = f"    echo \"127.0.0.1 {vmi_name}\" | tee -a /etc/hosts\n"    
    base_cloud_init += hostname_update
    print(base_cloud_init)


    # Append the registration command (if provided) to the cloud-init configuration
    if registration_command:
        base_cloud_init += f"\n  - |\n    {registration_command}"
        print(base_cloud_init)    

    # Define metadata and labels
    metadata = {
        "name": vmi_name,
        "namespace": namespace,
        "labels": {
            "kubevirt.io/vm": vmi_name
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
            "domain": {
                "cpu": {"cores": cpu_cores},
                "resources": {"requests": {"memory": ram}},
                "devices": {
                    "disks": [
                        {"name": "datavolumedisk1", "disk": {"bus": "virtio"}},
                        {"name": "cloudinitdisk", "disk": {"bus": "virtio"}}
                    ],
                    "interfaces": [
                        {"name": "default", "masquerade": {}}
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


def create_kubevirt_cluster(
    cluster_name,
    namespace,
    cluster_type="rke2",
    kubernetes_version="v1.31.4+rke2r1",
    vm_definitions=None
):
    """
    Creates a Rancher cluster via the provisioning API and then deploys VMIs on KubeVirt
    using the registration command. Each VM definition is a dictionary that must include:
      - vmi_name (str): Name of the VirtualMachineInstance.
      - disk_size (int or str): Disk size in Gi (default: 20).
      - disk_image (str): URL of the disk image.
      - cpu_cores (int): Number of CPU cores (default: 2).
      - ram (str): Amount of RAM (default: "2048M").
      - labeling_enabled (bool): Whether to add a label/nodeSelector (default: False).
      - label (str): The label key (default: "slices-gr").
      - multus_interfaces (list or None): A list of Multus configuration dictionaries.
      - roles (list): A list of roles, e.g. ["control plane", "worker"] or ["worker"].
    
    Workflow:
      1. Create the cluster.
      2. Retrieve the internal cluster name and poll until a valid registration command is available.
      3. Process VM definitions:
           a. Process master VM(s) first (those with role "control plane").
           b. For each VM, build the final registration command:
                 - Both roles: reg_command + " --etcd --controlplane --worker"
                 - Control plane only: reg_command + " --etcd --controlplane"
                 - Worker only: reg_command + " --worker"
           c. Use that final registration command as the parameter to create the VMI.
           d. Create a unique DataVolume and poll until it is ready.
           e. Create the VMI.
           f. Poll until the VMI is Running and confirm its registration via list_cluster_nodes_v3.
      4. Process worker VM(s) similarly.
    
    Returns:
      - True if all VMIs were deployed successfully, False otherwise.
    """
    # Step 1: Create the cluster.
    cluster_response = create_custom_cluster(
        cluster_name=cluster_name,
        cluster_type=cluster_type,
        kubernetes_version=kubernetes_version,
        namespace=namespace
    )
    if not cluster_response:
        print(f"[ERROR] Failed to create cluster '{cluster_name}' in namespace '{namespace}'.")
        return False
    print(f"[INFO] Cluster '{cluster_name}' created successfully in namespace '{namespace}'.")

    # Step 2: Retrieve internal cluster name and poll for registration command.
    internal_cluster_id = get_internal_cluster_name(namespace, cluster_name)
    if not internal_cluster_id:
        print("[ERROR] Unable to obtain internal cluster name.")
        return False

    reg_command_base = wait_for_registration_command(internal_cluster_id)
    if not reg_command_base:
        print("[ERROR] Unable to obtain registration command within the timeout period.")
        return False

    # Step 3: Separate VM definitions into master(s) and worker(s).
    if vm_definitions is None:
        vm_definitions = []

    master_vms = [vm for vm in vm_definitions if "control plane" in vm.get("roles", [])]
    worker_vms = [vm for vm in vm_definitions if ("worker" in vm.get("roles", [])) and ("control plane" not in vm.get("roles", []))]

    def build_registration_command(roles):
        if "control plane" in roles and "worker" in roles:
            return reg_command_base + " --etcd --controlplane --worker"
        elif "control plane" in roles:
            return reg_command_base + " --etcd --controlplane"
        elif "worker" in roles:
            return reg_command_base + " --worker"
        else:
            return ""

    # Step 4: Process master VM(s)
    for vm in master_vms:
        roles = vm.get("roles", [])
        final_reg_cmd = build_registration_command(roles)
        # For masters, we assume the registration command is appended directly.
        reg_to_pass = final_reg_cmd

        vmi_name = vm.get("vmi_name")
        if not vmi_name:
            print("[ERROR] Missing 'vmi_name' in VM definition.")
            continue

        disk_size = vm.get("disk_size", 20)
        storage_size = f"{disk_size}Gi" if isinstance(disk_size, (int, float)) else disk_size
        disk_image = vm.get("disk_image", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
        cpu_cores = vm.get("cpu_cores", 2)
        ram = vm.get("ram", "2048M")
        labeling_enabled = vm.get("labeling_enabled", False)
        label = vm.get("label", "slices-gr")
        multus_interfaces = vm.get("multus_interfaces", None)
        
        dv_name_vm = f"{vmi_name}-dv"
        if not create_data_volume(dv_name_vm, disk_image, storage_size=storage_size, namespace=namespace):
            print(f"[ERROR] Failed to create DataVolume '{dv_name_vm}' for master VMI '{vmi_name}'.")
            exit(1)

        # Wait until the DataVolume is ready.
        if not wait_for_dv_ready(dv_name_vm, namespace):
            print(f"[ERROR] DataVolume '{dv_name_vm}' is not ready for master VMI '{vmi_name}'.")
            exit(1)

        if not create_kubevirt_vmi(
            vmi_name=vmi_name,
            dv_name=dv_name_vm,
            namespace=namespace,
            cpu_cores=cpu_cores,
            ram=ram,
            labeling_enabled=labeling_enabled,
            label=label,
            multus_interfaces=multus_interfaces,
            registration_command=reg_to_pass
        ):
            print(f"[ERROR] Failed to create master VMI '{vmi_name}'.")
            continue

        print(f"[INFO] Master VMI '{vmi_name}' created. Waiting for it to reach Running state...")
        state = get_vmi_state(vmi_name, namespace)
        while state != "Running" and state is not None:
            print(f"[INFO] VMI '{vmi_name}' state: {state}. Retrying in 3 seconds...")
            time.sleep(3)
            state = get_vmi_state(vmi_name, namespace)

        nodes = list_cluster_nodes_v3(internal_cluster_id)
        found = any(vmi_name in (node.get("hostname", "") + node.get("name", "")) for node in nodes)
        while not found:
            print(f"[INFO] Waiting for master VMI '{vmi_name}' to appear in cluster nodes...")
            time.sleep(10)
            nodes = list_cluster_nodes_v3(internal_cluster_id)
            found = any(vmi_name in (node.get("hostname", "") + node.get("name", "")) for node in nodes)
        print(f"[INFO] Master VMI '{vmi_name}' is successfully registered in the cluster.")

    # Step 5: Process worker VM(s)
    for vm in worker_vms:
        roles = vm.get("roles", [])
        final_reg_cmd = build_registration_command(roles)
        cloud_init_userdata = final_reg_cmd

        vmi_name = vm.get("vmi_name")
        if not vmi_name:
            print("[ERROR] Missing 'vmi_name' in VM definition.")
            continue

        disk_size = vm.get("disk_size", 20)
        storage_size = f"{disk_size}Gi" if isinstance(disk_size, (int, float)) else disk_size
        disk_image = vm.get("disk_image", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
        cpu_cores = vm.get("cpu_cores", 2)
        ram = vm.get("ram", "2048M")
        labeling_enabled = vm.get("labeling_enabled", False)
        label = vm.get("label", "slices-gr")
        multus_interfaces = vm.get("multus_interfaces", None)
        
        dv_name_vm = f"{vmi_name}-dv"
        if not create_data_volume(dv_name_vm, disk_image, storage_size=storage_size, namespace=namespace):
            print(f"[ERROR] Failed to create DataVolume '{dv_name_vm}' for worker VMI '{vmi_name}'.")
            continue

        if not wait_for_dv_ready(dv_name_vm, namespace):
            print(f"[ERROR] DataVolume '{dv_name_vm}' is not ready for worker VMI '{vmi_name}'.")
            continue

        if not create_kubevirt_vmi(
            vmi_name=vmi_name,
            dv_name=dv_name_vm,
            namespace=namespace,
            cpu_cores=cpu_cores,
            ram=ram,
            labeling_enabled=labeling_enabled,
            label=label,
            multus_interfaces=multus_interfaces,
            registration_command=final_reg_cmd
        ):
            print(f"[ERROR] Failed to create worker VMI '{vmi_name}'.")
            continue

        print(f"[INFO] Worker VMI '{vmi_name}' created. Waiting for it to register in the cluster...")
        nodes = list_cluster_nodes_v3(internal_cluster_id)
        found = any(vmi_name in (node.get("hostname", "") + node.get("name", "")) for node in nodes)
        while not found:
            print(f"[INFO] Waiting for worker VMI '{vmi_name}' to appear in cluster nodes...")
            time.sleep(10)
            nodes = list_cluster_nodes_v3(internal_cluster_id)
            found = any(vmi_name in (node.get("hostname", "") + node.get("name", "")) for node in nodes)
        print(f"[INFO] Worker VMI '{vmi_name}' is successfully registered in the cluster.")

    return True


if __name__ == "__main__":
    # cluster_name = "new-test-new-op"
    # namespace = "default"
    # cluster_type = "rke2"
    # kubernetes_version = "v1.27.4+rke2r2"

    # vm_definitions = [
    #     {
    #         "vmi_name": "cluster-master",
    #         "disk_size": 15,
    #         "disk_image": "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
    #         "cpu_cores": 2,
    #         "ram": "2048M",
    #         "labeling_enabled": False,
    #         "label": "control-plane",
    #         "multus_interfaces": None,
    #         "roles": ["control plane", "worker"]  # Master has both roles in this example.
    #     },
    #     {
    #         "vmi_name": "worker1",
    #         "disk_size": 15,
    #         "disk_image": "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img",
    #         "cpu_cores": 2,
    #         "ram": "2048M",
    #         "labeling_enabled": False,
    #         "multus_interfaces": None,
    #         "roles": ["worker"]
    #     }
    # ]

    # create_kubevirt_cluster(
    #     cluster_name=cluster_name,
    #     namespace=namespace,
    #     cluster_type=cluster_type,
    #     kubernetes_version=kubernetes_version,
    #     vm_definitions=vm_definitions
    # )



     #delete_network_attachment_definition("net1", namespace="default")
     # delete_network_attachment_definition("net2", namespace="default")
     # delete_data_volume("ubuntu-data-volume-new")
     # delete_data_volume("ubuntu-data-volume-new1")
     # delete_data_volume("ubuntu-data-volume1")
     # delete_data_volume("ubuntu-data-volume2")
    #  delete_data_volume("ubuntu-data-volume3")
    
    create_namespace("testing-k3s")
    #delete_kubevirt_vmi("k3s-fresh")
    #delete_data_volume("k3s-fresh")
    #time.sleep(5)
    #create_custom_cluster(cluster_name="k3s-final-test-cluster", cluster_type="k3s", kubernetes_version="v1.31.4+k3s1", namespace="default", cni="flannel")
    create_data_volume(storage_size="18Gi", dv_name="k3s-change-dv", namespace="testing-k3s", image_url="https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
    create_kubevirt_vmi(
        vmi_name="k3s-change",
        dv_name="k3s-change-dv",
        namespace="testing-k3s",
        cpu_cores=8,
        ram="7105M",
        labeling_enabled=False,
        label="control-plane",
        multus_interfaces=None,
        registration_command="",
        dns_config_enabled=True,
        dns_config=["172.28.2.89"]
        )
      
    
    # delete_network_attachment_definition("net1")
    # delete_network_attachment_definition("net2")
    # create_data_volume("ubuntu-data-volume-new1", "https://cloud-images.ubuntu.com/focal/current/focal-server-cloudimg-amd64.img")
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
