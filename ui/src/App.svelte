<script>
  let playerName = "";
  let worldNumber = 1;
  let mqttUrl = "tcp://192.168.0.103:1883"; // Helpful default for you!
  let roomName = "panic-run";
  let connected = false;
  let ws;

  function handleConnect() {
    if (!playerName || !mqttUrl || !roomName) {
      alert("Please fill out all fields!");
      return;
    }

    ws = new WebSocket(`ws://${window.location.host}/ws`);

    ws.onopen = () => {
      connected = true;
      console.log("Connected to Go backend!");

      // Send the full config payload
      const config = {
        playerName: playerName,
        worldNumber: worldNumber,
        mqttUrl: mqttUrl,
        roomName: roomName
      };
      ws.send(JSON.stringify(config));
    };

    ws.onmessage = (event) => {
      console.log("Received from Go:", event.data);
    };

    ws.onclose = () => {
      connected = false;
      alert("Lost connection to the tracker backend.");
    };
  }
</script>

<main class="container">
  <h1>Print & Panic Multiworld</h1>

  {#if !connected}
    <div class="card">
      <div class="input-group">
        <label for="playerName">Player Name</label>
        <input id="playerName" type="text" bind:value={playerName} placeholder="e.g., Navi" />
      </div>

      <div class="input-group">
        <label for="worldNumber">World Number (Player ID)</label>
        <input id="worldNumber" type="number" bind:value={worldNumber} min="1" max="255" />
      </div>

      <div class="input-group">
        <label for="mqttUrl">MQTT Broker URL</label>
        <input id="mqttUrl" type="text" bind:value={mqttUrl} placeholder="tcp://broker.hivemq.com:1883" />
      </div>

      <div class="input-group">
        <label for="roomName">Room Name</label>
        <input id="roomName" type="text" bind:value={roomName} placeholder="Secret password" />
      </div>

      <button on:click={handleConnect}>Connect to ROM</button>
    </div>
  {:else}
    <div class="card success">
      <h2>Connected!</h2>
      <p>Tracking items for <strong>{playerName}</strong> (World {worldNumber})</p>
      <p>Room: <em>{roomName}</em></p>
      <div class="pulse-indicator">Routing items through the cloud...</div>
    </div>
  {/if}
</main>

<style>
  .container { font-family: system-ui, sans-serif; max-width: 400px; margin: 2rem auto; text-align: center; color: white; background: #1e1e1e; padding: 2rem; border-radius: 12px; }
  .card { background: #2a2a2a; padding: 1.5rem; border-radius: 8px; margin-top: 1rem; }
  .input-group { margin-bottom: 1rem; text-align: left; }
  label { display: block; margin-bottom: 0.5rem; font-size: 0.9rem; color: #aaa; }
  input { width: 100%; padding: 0.5rem; border-radius: 4px; border: 1px solid #444; background: #111; color: white; box-sizing: border-box; }
  button { width: 100%; padding: 0.75rem; background: #4CAF50; color: white; border: none; border-radius: 4px; font-weight: bold; cursor: pointer; }
  button:hover { background: #45a049; }
  .success { border: 1px solid #4CAF50; }
</style>