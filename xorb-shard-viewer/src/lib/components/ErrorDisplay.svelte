<script lang="ts">
  export let error: string;
  export let filename: string = "";
</script>

<div class="error-container">
  <div class="error-content">
    <div class="error-icon">⚠️</div>
    <h3>Error Parsing File</h3>
    {#if filename}
      <p class="filename">File: {filename}</p>
    {/if}
    <div class="error-message">
      {error}
    </div>
    <div class="suggestions">
      <h4>Suggestions:</h4>
      <ul>
        <li>Verify that the file is a valid XORB or Shard file</li>
        <li>Check that the file is not corrupted</li>
        <li>Ensure the file format matches the expected binary structure</li>
      </ul>
    </div>
  </div>
</div>

<style>
  .error-container {
    max-width: 600px;
    margin: 20px auto;
    padding: 20px;
  }

  .error-content {
    background: #fee;
    border: 1px solid #fcc;
    border-radius: 8px;
    padding: 30px;
    text-align: center;
  }

  .error-icon {
    font-size: 48px;
    margin-bottom: 16px;
  }

  .error-content h3 {
    margin: 0 0 16px 0;
    color: #d32f2f;
    font-size: 20px;
    font-weight: 600;
  }

  .filename {
    font-family: "Monaco", "Consolas", monospace;
    background: #fff;
    padding: 8px 12px;
    border-radius: 4px;
    margin: 0 0 20px 0;
    color: #666;
    border: 1px solid #eee;
  }

  .error-message {
    background: #fff;
    border: 1px solid #fcc;
    border-radius: 4px;
    padding: 16px;
    margin: 20px 0;
    font-family: "Monaco", "Consolas", monospace;
    font-size: 14px;
    color: #d32f2f;
    text-align: left;
    white-space: pre-wrap;
  }

  .suggestions {
    text-align: left;
    margin-top: 20px;
    background: #fff9c4;
    border: 1px solid #f7e97d;
    border-radius: 4px;
    padding: 16px;
  }

  .suggestions h4 {
    margin: 0 0 12px 0;
    color: #8d6e00;
    font-size: 16px;
  }

  .suggestions ul {
    margin: 0;
    padding-left: 20px;
    color: #8d6e00;
  }

  .suggestions li {
    margin-bottom: 8px;
  }
</style>
