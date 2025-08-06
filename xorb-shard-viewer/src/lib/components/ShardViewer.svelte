<script lang="ts">
  import type { ShardData } from "../types.js";
  import {
    formatBytes,
    formatHash,
    formatHashShort,
    formatTimestamp,
  } from "../parsers.js";

  export let data: ShardData;
  export let filename: string;
  export let fileSize: number;

  $: header = data.header;
  $: footer = data.footer;
  $: totalFiles = data.file_info.length;
  $: totalXorbs = data.cas_info.length;
  $: totalChunks = data.cas_info.reduce(
    (sum, cas) => sum + cas.header.num_entries,
    0
  );
  $: totalStoredBytes = data.cas_info.reduce(
    (sum, cas) => sum + cas.header.num_bytes_in_cas,
    0
  );
</script>

<div class="shard-viewer">
  <div class="header">
    <h2>🗂️ Shard File Analysis</h2>
    <div class="file-info">
      <span class="filename">{filename}</span>
      <span class="file-size">{formatBytes(fileSize)}</span>
    </div>
  </div>

  <div class="metadata-grid">
    <div class="metadata-section">
      <h3>Header Information</h3>
      <div class="metadata-item">
        <span class="label">Version:</span>
        <span class="value">{header.version}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Footer Size:</span>
        <span class="value">{formatBytes(header.footer_size)}</span>
      </div>
    </div>

    <div class="metadata-section">
      <h3>Content Statistics</h3>
      <div class="metadata-item">
        <span class="label">Files:</span>
        <span class="value">{totalFiles.toLocaleString()}</span>
      </div>
      <div class="metadata-item">
        <span class="label">XORBs:</span>
        <span class="value">{totalXorbs.toLocaleString()}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Total Chunks:</span>
        <span class="value">{totalChunks.toLocaleString()}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Stored Bytes:</span>
        <span class="value">{formatBytes(totalStoredBytes)}</span>
      </div>
    </div>

    <div class="metadata-section">
      <h3>Timestamps & Security</h3>
      <div class="metadata-item">
        <span class="label">Created:</span>
        <span class="value"
          >{formatTimestamp(footer.shard_creation_timestamp)}</span
        >
      </div>
      <div class="metadata-item">
        <span class="label">Key Expiry:</span>
        <span class="value">{formatTimestamp(footer.shard_key_expiry)}</span>
      </div>
      <div class="metadata-item">
        <span class="label">HMAC Key:</span>
        <span class="value hash" title={formatHash(footer.chunk_hash_hmac_key)}>
          {formatHashShort(footer.chunk_hash_hmac_key)}
        </span>
      </div>
    </div>
  </div>

  {#if data.file_info.length > 0}
    <div class="file-details">
      <h3>File Information ({data.file_info.length} files)</h3>
      <div class="table-container">
        <table class="data-table">
          <thead>
            <tr>
              <th>File Hash</th>
              <th>Flags</th>
              <th>Entries</th>
              <th>Has Verification</th>
              <th>Has Metadata</th>
            </tr>
          </thead>
          <tbody>
            {#each data.file_info as fileInfo}
              <tr>
                <td class="hash" title={formatHash(fileInfo.header.file_hash)}>
                  {formatHashShort(fileInfo.header.file_hash)}
                </td>
                <td
                  >{fileInfo.header.file_flags
                    .toString(16)
                    .padStart(8, "0")}</td
                >
                <td>{fileInfo.header.num_entries}</td>
                <td>{fileInfo.header.file_flags & 0x80000000 ? "✅" : "❌"}</td>
                <td>{fileInfo.header.file_flags & 0x40000000 ? "✅" : "❌"}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </div>
  {/if}

  {#if data.cas_info.length > 0}
    <div class="cas-details">
      <h3>CAS Information ({data.cas_info.length} XORBs)</h3>
      <div class="table-container">
        <table class="data-table">
          <thead>
            <tr>
              <th>CAS Hash</th>
              <th>Chunks</th>
              <th>Bytes in CAS</th>
              <th>Bytes on Disk</th>
              <th>Compression</th>
            </tr>
          </thead>
          <tbody>
            {#each data.cas_info as casInfo}
              <tr>
                <td class="hash" title={formatHash(casInfo.header.cas_hash)}>
                  {formatHashShort(casInfo.header.cas_hash)}
                </td>
                <td>{casInfo.header.num_entries}</td>
                <td>{formatBytes(casInfo.header.num_bytes_in_cas)}</td>
                <td>{formatBytes(casInfo.header.num_bytes_on_disk)}</td>
                <td>
                  {casInfo.header.num_bytes_in_cas > 0
                    ? (
                        (casInfo.header.num_bytes_on_disk /
                          casInfo.header.num_bytes_in_cas) *
                        100
                      ).toFixed(1) + "%"
                    : "N/A"}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </div>
  {/if}
</div>

<style>
  .shard-viewer {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 30px;
    padding-bottom: 20px;
    border-bottom: 2px solid #eee;
  }

  .header h2 {
    margin: 0;
    color: #333;
    font-size: 24px;
  }

  .file-info {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 4px;
  }

  .filename {
    font-weight: 600;
    color: #555;
  }

  .file-size {
    font-size: 14px;
    color: #888;
  }

  .metadata-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
    margin-bottom: 30px;
  }

  .metadata-section {
    background: #f8f9fa;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid #e9ecef;
  }

  .metadata-section h3 {
    margin: 0 0 15px 0;
    color: #495057;
    font-size: 18px;
    font-weight: 600;
  }

  .metadata-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    padding: 8px 0;
    border-bottom: 1px solid #e9ecef;
  }

  .metadata-item:last-child {
    border-bottom: none;
    margin-bottom: 0;
  }

  .metadata-item .label {
    font-weight: 500;
    color: #6c757d;
    margin-right: 10px;
  }

  .metadata-item .value {
    font-family: "Monaco", "Consolas", monospace;
    font-size: 14px;
    color: #212529;
    text-align: right;
  }

  .hash {
    font-family: "Monaco", "Consolas", monospace;
    background: #e9ecef;
    padding: 2px 6px;
    border-radius: 4px;
    cursor: help;
    font-size: 12px;
  }

  .file-details,
  .cas-details {
    margin-top: 30px;
  }

  .file-details h3,
  .cas-details h3 {
    margin: 0 0 20px 0;
    color: #495057;
    font-size: 18px;
    font-weight: 600;
  }

  .table-container {
    overflow-x: auto;
    border-radius: 8px;
    border: 1px solid #e9ecef;
  }

  .data-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
  }

  .data-table th,
  .data-table td {
    padding: 12px;
    text-align: left;
    border-bottom: 1px solid #e9ecef;
  }

  .data-table th {
    background: #f8f9fa;
    font-weight: 600;
    color: #495057;
  }

  .data-table tr:hover {
    background: #f8f9fa;
  }

  @media (max-width: 768px) {
    .header {
      flex-direction: column;
      align-items: flex-start;
      gap: 10px;
    }

    .metadata-grid {
      grid-template-columns: 1fr;
    }

    .metadata-item {
      flex-direction: column;
      align-items: flex-start;
      gap: 4px;
    }

    .metadata-item .value {
      text-align: left;
    }
  }
</style>
