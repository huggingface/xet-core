<script lang="ts">
  import type { Chunk } from "../types.js";
  import { formatBytes } from "../parsers.js";

  export let data: Chunk[];
  export let filename: string;
  export let fileSize: number;

  $: totalChunks = data.length;
  $: totalCompressedSize = data.reduce(
    (sum, chunk) => sum + chunk.header.compressed_size,
    0
  );
  $: totalUncompressedSize = data.reduce(
    (sum, chunk) => sum + chunk.header.uncompressed_size,
    0
  );
  $: averageChunkSize =
    totalChunks > 0 ? totalUncompressedSize / totalChunks : 0;
  $: compressionRatio =
    totalUncompressedSize > 0
      ? (totalCompressedSize / totalUncompressedSize) * 100
      : 0;

  function getCompressionTypeName(type: number): string {
    switch (type) {
      case 0:
        return "None";
      case 1:
        return "LZ4";
      case 2:
        return "Zstd";
      case 3:
        return "Gzip";
      default:
        return `Unknown (${type})`;
    }
  }
</script>

<div class="xorb-viewer">
  <div class="header">
    <h2>📦 XORB File Analysis</h2>
    <div class="file-info">
      <span class="filename">{filename}</span>
      <span class="file-size">{formatBytes(fileSize)}</span>
    </div>
  </div>

  <div class="metadata-grid">
    <div class="metadata-section">
      <h3>File Information</h3>
      <div class="metadata-item">
        <span class="label">File Size:</span>
        <span class="value">{formatBytes(fileSize)}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Total Chunks:</span>
        <span class="value">{totalChunks.toLocaleString()}</span>
      </div>
    </div>

    <div class="metadata-section">
      <h3>Compression Statistics</h3>
      <div class="metadata-item">
        <span class="label">Total Compressed Size:</span>
        <span class="value">{formatBytes(totalCompressedSize)}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Total Uncompressed Size:</span>
        <span class="value">{formatBytes(totalUncompressedSize)}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Overall Compression Ratio:</span>
        <span class="value">{compressionRatio.toFixed(1)}%</span>
      </div>
      <div class="metadata-item">
        <span class="label">Average Chunk Size:</span>
        <span class="value">{formatBytes(averageChunkSize)}</span>
      </div>
    </div>
  </div>

  {#if data.length > 0}
    <div class="chunk-details">
      <h3>Chunk Headers ({data.length} chunks)</h3>
      <div class="chunk-table-container">
        <table class="chunk-table">
          <thead>
            <tr>
              <th>Index</th>
              <th>Version</th>
              <th>Compression Type</th>
              <th>Compressed Size</th>
              <th>Uncompressed Size</th>
              <th>Compression Ratio</th>
            </tr>
          </thead>
          <tbody>
            {#each data as chunk, i}
              <tr>
                <td>{i}</td>
                <td>{chunk.header.version}</td>
                <td>{getCompressionTypeName(chunk.header.compression_type)}</td>
                <td>{formatBytes(chunk.header.compressed_size)}</td>
                <td>{formatBytes(chunk.header.uncompressed_size)}</td>
                <td>
                  {chunk.header.uncompressed_size > 0
                    ? (
                        (chunk.header.compressed_size /
                          chunk.header.uncompressed_size) *
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
  .xorb-viewer {
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

  .chunk-details {
    margin-top: 30px;
  }

  .chunk-details h3 {
    margin: 0 0 20px 0;
    color: #495057;
    font-size: 18px;
    font-weight: 600;
  }

  .chunk-table-container {
    overflow-x: auto;
    border-radius: 8px;
    border: 1px solid #e9ecef;
  }

  .chunk-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
  }

  .chunk-table th,
  .chunk-table td {
    padding: 12px;
    text-align: left;
    border-bottom: 1px solid #e9ecef;
  }

  .chunk-table th {
    background: #f8f9fa;
    font-weight: 600;
    color: #495057;
  }

  .chunk-table tr:hover {
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
