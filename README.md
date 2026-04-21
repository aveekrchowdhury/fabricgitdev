# fabricgitdev

## Deployment Steps: Copy SQL Files to Lakehouse

Here are **two scriptable ways** to copy your `.sql` files into the **Lakehouse ➜ Files** area (so your post-deploy notebook can read/execute them). The **recommended** approach for CI/CD is **AzCopy to OneLake**. Microsoft explicitly documents using **AzCopy with OneLake** and calling out `--trusted-microsoft-suffixes` for `fabric.microsoft.com`. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy), [[blog.fabric.microsoft.com]](https://blog.fabric.microsoft.com/en-us/blog/load-data-from-network-protected-azure-storage-accounts-to-microsoft-onelake-with-azcopy?ft=All)

***

## Option A (Recommended): **AzCopy → OneLake "Files" path** (CI/CD friendly)

### 1) Authenticate AzCopy

```bash
azcopy login
```

Microsoft's OneLake+AzCopy guidance uses `azcopy login` first. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy)

### 2) Copy from your build artifact folder (repo checkout) into the Lakehouse "Files" folder

Use the OneLake DFS endpoint format shown in Microsoft's blog example (note the `/Files/...` path). [[blog.fabric.microsoft.com]](https://blog.fabric.microsoft.com/en-us/blog/load-data-from-network-protected-azure-storage-accounts-to-microsoft-onelake-with-azcopy?ft=All)

```bash
azcopy copy "./scripts/*.sql" ^
  "https://onelake.dfs.fabric.microsoft.com/<WORKSPACE>/<LAKEHOUSE>.Lakehouse/Files/scripts/" ^
  --recursive ^
  --trusted-microsoft-suffixes "fabric.microsoft.com"
```

Key points (straight from Microsoft guidance):

*   OneLake supports Azure Storage APIs/tools, so **AzCopy works**. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy)
*   Add `fabric.microsoft.com` to trusted suffixes when using AzCopy with OneLake. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy), [[blog.fabric.microsoft.com]](https://blog.fabric.microsoft.com/en-us/blog/load-data-from-network-protected-azure-storage-accounts-to-microsoft-onelake-with-azcopy?ft=All)

> Tip: If you want to copy between workspaces/lakehouses (dev → test), Microsoft Learn also includes a sample for copying data between Fabric workspaces using `azcopy copy`. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy)

### 3) Verify what landed (optional)

```bash
azcopy list "https://onelake.dfs.fabric.microsoft.com/<WORKSPACE>/<LAKEHOUSE>.Lakehouse/Files/scripts/" --trusted-microsoft-suffixes "fabric.microsoft.com"
```

(Using AzCopy for upload/download/list is within the scope of the OneLake+AzCopy article.) [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy)

***

## Option B: **Upload via Lakehouse REST API** (for automation without AzCopy)

Microsoft's REST API article is about managing a lakehouse programmatically and includes patterns you can adapt for automation scripts.   
If you're already using REST in your pipeline (e.g., you're calling "run on demand notebook job"), you can also use REST to manage lakehouse content. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api)

A commonly used pattern is **PUT binary content** into a `/Files/...` path (your pipeline can `curl --data-binary @file.sql`). See Microsoft's lakehouse REST API overview to ground the approach (token + workspaceId + lakehouseId patterns). [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api)

Example (pipeline step style):

```bash
curl -X PUT \
  "https://api.fabric.microsoft.com/v1/workspaces/<workspaceId>/lakehouses/<lakehouseId>/files/scripts/create_customers_table.sql" \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @./scripts/create_customers_table.sql
```

> If you go this route, keep using the documented requirement: obtain a Microsoft Entra token and include it in the `Authorization` header, and substitute `{workspaceId}` / `{lakehouseId}` accordingly. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api)

***

## How this fits your Lakehouse CI/CD pattern

The CI/CD flow follows this pattern: **deploy workspace items → copy SQL files to Lakehouse Files → run post-deployment notebook**.   

This "copy SQL files" step is exactly what Option A/B implements above.

***

## Drop-in Azure DevOps YAML step (AzCopy)

Here's a copy/paste step you can add before your "RunNotebook" stage:

```yaml
- script: |
    azcopy login
    azcopy copy "$(Build.SourcesDirectory)/scripts/*.sql" "https://onelake.dfs.fabric.microsoft.com/<WORKSPACE>/<LAKEHOUSE>.Lakehouse/Files/scripts/" --recursive --trusted-microsoft-suffixes "fabric.microsoft.com"
  displayName: "Copy DDL SQL files to Lakehouse Files (OneLake) via AzCopy"
```

The `--trusted-microsoft-suffixes "fabric.microsoft.com"` requirement is directly called out in Microsoft's OneLake+AzCopy guidance. [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy), [[blog.fabric.microsoft.com]](https://blog.fabric.microsoft.com/en-us/blog/load-data-from-network-protected-azure-storage-accounts-to-microsoft-onelake-with-azcopy?ft=All)

***

## Post-Deployment: Run the Notebook

After copying the SQL files to the Lakehouse Files area, **run the post-deployment notebook** (e.g., `create_lh_tables`) to execute the SQL scripts and create the necessary tables in your Lakehouse.

The notebook can read the SQL files from the Files area and execute them against the Lakehouse to set up your data structures.

***

### Configuration Notes

**Path format options:**

* **Name form**: `<workspaceName>/<lakehouseName>.Lakehouse/...`  
* **GUID form**: `<workspaceId>/<lakehouseId>.Lakehouse/...`

Both appear in OneLake path patterns. Use whichever matches your environment conventions. [[blog.fabric.microsoft.com]](https://blog.fabric.microsoft.com/en-us/blog/load-data-from-network-protected-azure-storage-accounts-to-microsoft-onelake-with-azcopy?ft=All), [[learn.microsoft.com]](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azcopy)
