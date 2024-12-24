# Databricks notebook source
storage_acc_name="storageaccgen2project1"
folder_path="helathcare-revenue-cycle-managment/project"
container_name="helathcare-revenue-cycle-managment"
storage_access_keys="**secrete**"

mount_points=["bronze","gold","silver","landing","configs"]
for mounts in mount_points:
    if any(x.mountPoint == f"/mnt/{mounts}" for x in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{mounts}")
    if not any(mount.mountPoint == f"/mnt/{mounts}" for mount in dbutils.fs.mounts()):
        try:
           dbutils.fs.mount(
                source=f"wasbs://{container_name}@{storage_acc_name}.blob.core.windows.net/project/{mounts}",
                mount_point=f"/mnt/{mounts}",
                extra_configs={f"fs.azure.account.key.{storage_acc_name}.blob.core.windows.net": storage_access_keys}
            )
        except expection as e:
            print("mount exception",e)
