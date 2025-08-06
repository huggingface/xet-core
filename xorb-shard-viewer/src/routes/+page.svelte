<script lang="ts">
  import FileUpload from "$lib/components/FileUpload.svelte";
  import XorbViewer from "$lib/components/XorbViewer.svelte";
  import ShardViewer from "$lib/components/ShardViewer.svelte";
  import ErrorDisplay from "$lib/components/ErrorDisplay.svelte";
  import { parseFile } from "$lib/parsers.js";
  import type { ParsedFileMetadata, Chunk, ShardData } from "$lib/types.js";

  let parsedData: ParsedFileMetadata | null = null;
  let loading = false;

  async function handleFileSelected(
    event: CustomEvent<{ file: File; type: "xorb" | "shard" }>
  ) {
    loading = true;
    parsedData = null;

    try {
      const { file, type } = event.detail;
      const result = await parseFile(file, type);
      parsedData = result;
    } catch (error) {
      parsedData = {
        type: event.detail.type,
        filename: event.detail.file.name,
        fileSize: event.detail.file.size,
        data: {} as any,
        error:
          error instanceof Error ? error.message : "Unknown error occurred",
      };
    } finally {
      loading = false;
    }
  }

  function resetView() {
    parsedData = null;
    loading = false;
  }
</script>

<svelte:head>
  <title>XORB & Shard File Viewer</title>
  <meta
    name="description"
    content="Parse and view metadata from XORB and Shard object files"
  />
</svelte:head>

<main>
  <div class="container">
    <header>
      <h1>🔍 XORB & Shard File Viewer</h1>
      <p>
        Upload and analyze XORB or Shard object files to view their metadata
        structure
      </p>
      {#if parsedData}
        <button class="reset-btn" on:click={resetView}>
          ← Upload New File
        </button>
      {/if}
    </header>

    {#if loading}
      <div class="loading">
        <div class="spinner"></div>
        <p>Parsing file...</p>
      </div>
    {:else if parsedData}
      {#if parsedData.error}
        <ErrorDisplay error={parsedData.error} filename={parsedData.filename} />
      {:else if parsedData.type === "xorb"}
        <XorbViewer
          data={parsedData.data as Chunk[]}
          filename={parsedData.filename}
          fileSize={parsedData.fileSize}
        />
      {:else if parsedData.type === "shard"}
        <ShardViewer
          data={parsedData.data as ShardData}
          filename={parsedData.filename}
          fileSize={parsedData.fileSize}
        />
      {/if}
    {:else}
      <FileUpload on:fileSelected={handleFileSelected} />

      <div class="info-section">
        <h2>Supported File Types</h2>
        <div class="file-types">
          <div class="file-type">
            <h3>📦 XORB Files</h3>
            <p>
              XORB (Xet Orb) files contain collections of compressed chunks with
              metadata. The viewer will display chunk information, compression
              statistics, and structural details.
            </p>
            <ul>
              <li>Chunk count and sizes</li>
              <li>Compression ratios</li>
              <li>Hash information</li>
              <li>Boundary offsets</li>
            </ul>
          </div>

          <div class="file-type">
            <h3>🗂️ Shard Files</h3>
            <p>
              MDB Shard files store file metadata and content-addressable
              storage information for efficient deduplication. The viewer shows
              file and CAS details.
            </p>
            <ul>
              <li>File information entries</li>
              <li>CAS (Content Addressable Storage) data</li>
              <li>Lookup tables</li>
              <li>Timestamps and security keys</li>
            </ul>
          </div>
        </div>
      </div>
    {/if}
  </div>
</main>

<style>
  :global(body) {
    margin: 0;
    padding: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
      sans-serif;
    background: #f5f7fa;
    color: #333;
    line-height: 1.6;
  }

  :global(*) {
    box-sizing: border-box;
  }

  main {
    min-height: 100vh;
    padding: 20px;
  }

  .container {
    max-width: 1200px;
    margin: 0 auto;
  }

  header {
    text-align: center;
    margin-bottom: 40px;
    position: relative;
  }

  header h1 {
    font-size: 36px;
    margin: 0 0 16px 0;
    color: #2c3e50;
    font-weight: 700;
  }

  header p {
    font-size: 18px;
    color: #6c757d;
    margin: 0;
  }

  .reset-btn {
    position: absolute;
    left: 0;
    top: 50%;
    transform: translateY(-50%);
    background: #007bff;
    color: white;
    border: none;
    padding: 10px 20px;
    border-radius: 6px;
    cursor: pointer;
    font-size: 14px;
    font-weight: 500;
    transition: background-color 0.2s;
  }

  .reset-btn:hover {
    background: #0056b3;
  }

  .loading {
    text-align: center;
    padding: 60px 20px;
  }

  .spinner {
    width: 40px;
    height: 40px;
    border: 4px solid #f3f3f3;
    border-top: 4px solid #007bff;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin: 0 auto 20px;
  }

  @keyframes spin {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }

  .loading p {
    color: #6c757d;
    font-size: 16px;
    margin: 0;
  }

  .info-section {
    margin-top: 60px;
    text-align: center;
  }

  .info-section h2 {
    font-size: 24px;
    color: #2c3e50;
    margin: 0 0 30px 0;
  }

  .file-types {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
    gap: 30px;
    margin-top: 30px;
  }

  .file-type {
    background: white;
    padding: 30px;
    border-radius: 12px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    text-align: left;
  }

  .file-type h3 {
    font-size: 20px;
    margin: 0 0 16px 0;
    color: #2c3e50;
  }

  .file-type p {
    color: #6c757d;
    margin: 0 0 20px 0;
    line-height: 1.6;
  }

  .file-type ul {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .file-type li {
    padding: 8px 0;
    border-bottom: 1px solid #eee;
    color: #495057;
  }

  .file-type li:before {
    content: "✓";
    color: #28a745;
    font-weight: bold;
    margin-right: 8px;
  }

  .file-type li:last-child {
    border-bottom: none;
  }

  @media (max-width: 768px) {
    header h1 {
      font-size: 28px;
    }

    header p {
      font-size: 16px;
    }

    .reset-btn {
      position: static;
      transform: none;
      margin-top: 20px;
    }

    .file-types {
      grid-template-columns: 1fr;
    }

    .file-type {
      padding: 20px;
    }
  }
</style>
