<script lang="ts">
  import { createEventDispatcher } from "svelte";

  const dispatch = createEventDispatcher<{
    fileSelected: { file: File; type: "xorb" | "shard" };
  }>();

  let xorbDragActive = false;
  let shardDragActive = false;
  let xorbFileInput: HTMLInputElement;
  let shardFileInput: HTMLInputElement;

  function handleDragOver(event: DragEvent, type: "xorb" | "shard") {
    event.preventDefault();
    if (type === "xorb") {
      xorbDragActive = true;
    } else {
      shardDragActive = true;
    }
  }

  function handleDragLeave(type: "xorb" | "shard") {
    if (type === "xorb") {
      xorbDragActive = false;
    } else {
      shardDragActive = false;
    }
  }

  function handleDrop(event: DragEvent, type: "xorb" | "shard") {
    event.preventDefault();
    if (type === "xorb") {
      xorbDragActive = false;
    } else {
      shardDragActive = false;
    }

    const files = event.dataTransfer?.files;
    if (files && files.length > 0) {
      handleFile(files[0], type);
    }
  }

  function handleFileInput(event: Event, type: "xorb" | "shard") {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      handleFile(input.files[0], type);
    }
  }

  function handleFile(file: File, type: "xorb" | "shard") {
    dispatch("fileSelected", { file, type });
  }

  function openFileDialog(type: "xorb" | "shard") {
    if (type === "xorb") {
      xorbFileInput.click();
    } else {
      shardFileInput.click();
    }
  }
</script>

<div class="upload-container">
  <div class="upload-areas">
    <!-- XORB Upload Area -->
    <div class="upload-section">
      <h3>📦 Upload XORB File</h3>
      <div
        class="upload-area xorb-area"
        class:drag-active={xorbDragActive}
        on:dragover={(e) => handleDragOver(e, "xorb")}
        on:dragleave={() => handleDragLeave("xorb")}
        on:drop={(e) => handleDrop(e, "xorb")}
        on:click={() => openFileDialog("xorb")}
        on:keydown={(e) => e.key === "Enter" && openFileDialog("xorb")}
        role="button"
        tabindex="0"
      >
        <div class="upload-content">
          <div class="upload-icon">📦</div>
          <h4>Drop XORB file here</h4>
          <p>or click to browse</p>
          <span class="format-tag xorb-tag">XORB</span>
        </div>
      </div>
    </div>

    <!-- Shard Upload Area -->
    <div class="upload-section">
      <h3>🗂️ Upload Shard File</h3>
      <div
        class="upload-area shard-area"
        class:drag-active={shardDragActive}
        on:dragover={(e) => handleDragOver(e, "shard")}
        on:dragleave={() => handleDragLeave("shard")}
        on:drop={(e) => handleDrop(e, "shard")}
        on:click={() => openFileDialog("shard")}
        on:keydown={(e) => e.key === "Enter" && openFileDialog("shard")}
        role="button"
        tabindex="0"
      >
        <div class="upload-content">
          <div class="upload-icon">🗂️</div>
          <h4>Drop Shard file here</h4>
          <p>or click to browse</p>
          <span class="format-tag shard-tag">SHARD</span>
        </div>
      </div>
    </div>
  </div>

  <input
    bind:this={xorbFileInput}
    type="file"
    on:change={(e) => handleFileInput(e, "xorb")}
    style="display: none;"
  />

  <input
    bind:this={shardFileInput}
    type="file"
    on:change={(e) => handleFileInput(e, "shard")}
    style="display: none;"
  />
</div>

<style>
  .upload-container {
    width: 100%;
    max-width: 800px;
    margin: 0 auto;
  }

  .upload-areas {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 30px;
  }

  .upload-section {
    text-align: center;
  }

  .upload-section h3 {
    margin: 0 0 20px 0;
    color: #2c3e50;
    font-size: 18px;
    font-weight: 600;
  }

  .upload-area {
    border: 2px dashed #ccc;
    border-radius: 8px;
    padding: 30px 20px;
    text-align: center;
    cursor: pointer;
    transition: all 0.3s ease;
    background: #fafafa;
    min-height: 200px;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .xorb-area:hover {
    border-color: #28a745;
    background: #f0fff4;
  }

  .xorb-area.drag-active {
    border-color: #28a745;
    background: #e8f5e8;
    transform: scale(1.02);
  }

  .shard-area:hover {
    border-color: #007bff;
    background: #f0f8ff;
  }

  .shard-area.drag-active {
    border-color: #007bff;
    background: #e3f2fd;
    transform: scale(1.02);
  }

  .upload-content {
    pointer-events: none;
  }

  .upload-icon {
    font-size: 36px;
    margin-bottom: 12px;
  }

  .upload-area h4 {
    margin: 0 0 8px 0;
    color: #333;
    font-weight: 600;
    font-size: 16px;
  }

  .upload-area p {
    margin: 0 0 16px 0;
    color: #666;
    font-size: 14px;
  }

  .format-tag {
    color: white;
    padding: 4px 12px;
    border-radius: 16px;
    font-size: 12px;
    font-weight: 600;
  }

  .xorb-tag {
    background: #28a745;
  }

  .shard-tag {
    background: #007bff;
  }

  @media (max-width: 768px) {
    .upload-areas {
      grid-template-columns: 1fr;
      gap: 20px;
    }

    .upload-area {
      padding: 20px 15px;
      min-height: 150px;
    }

    .upload-icon {
      font-size: 28px;
    }
  }
</style>
