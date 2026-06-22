import os
import argparse
from pathlib import Path
from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


def get_service_client(account_name: str):
    account_url = f"https://{account_name}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)


def ensure_dir(client, path: str):
    try:
        client.create_directory(path)
    except Exception:
        pass


def upload_file(file_client, local_path: Path):
    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)


def upload_directory(account_name: str, file_system: str, local_root: str, remote_root: str):
    service_client = get_service_client(account_name)
    fs_client = service_client.get_file_system_client(file_system=file_system)

    local_root_path = Path(local_root).resolve()
    ensure_dir(fs_client, remote_root)

    uploaded = []
    for root, _, files in os.walk(local_root_path):
        for name in files:
            local_file = Path(root) / name
            rel_path = local_file.relative_to(local_root_path).as_posix()
            remote_path = f"{remote_root}/{rel_path}".replace("//", "/")

            dir_path = str(Path(remote_path).parent).replace("\\", "/")
            ensure_dir(fs_client, dir_path)

            file_client = fs_client.get_file_client(remote_path)
            upload_file(file_client, local_file)
            uploaded.append(remote_path)
            print(f"uploaded: {local_file} -> {remote_path}")

    return uploaded


def main():
    parser = argparse.ArgumentParser(description="Upload a local folder to ADLS Gen2.")
    parser.add_argument("--account-name", required=True)
    parser.add_argument("--file-system", required=True)
    parser.add_argument("--local-root", required=True)
    parser.add_argument("--remote-root", required=True, default="landing/raw")
    args = parser.parse_args()

    start = datetime.now(timezone.utc)
    uploaded = upload_directory(
        account_name=args.account_name,
        file_system=args.file_system,
        local_root=args.local_root,
        remote_root=args.remote_root
    )
    end = datetime.now(timezone.utc)

    print(f"\ncompleted: {len(uploaded)} files")
    print(f"start: {start.isoformat()}")
    print(f"end: {end.isoformat()}")


if __name__ == "__main__":
    main()
