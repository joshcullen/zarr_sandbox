import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { ZarrLayer } from '@carbonplan/zarr-layer';

// ==========================================
// 1. Initialize the MapLibre Map
// ==========================================
const map = new maplibregl.Map({
  container: 'map',
  style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  center: [-80, 25], // Centered roughly on Florida/Bahamas
  zoom: 6, // Zoomed in enough to only load 2-4 chunks
});

// We set the initial projection to globe so it matches our widget's default state
map.once('load', () => {
  map.setProjection({ type: 'globe' });
});

// Declare the layer globally so sliders and clicks can always find the "active" WebGL context
window.zarrLayer = null;
const LAYER_ID = 'cmems-data-layer';

// ==========================================
// 2. Layer Factory Function
// ==========================================
// This function stamps out a fresh WebGL layer whenever we need one,
// reading the current sliders so it remembers the user's selected time/depth.
function createZarrLayer() {
  const timeSlider = document.getElementById('time-slider');
  const zSlider = document.getElementById('z-slider');
  
  const currentTime = timeSlider ? parseInt(timeSlider.value, 10) : 0;
  const currentZ = zSlider ? parseInt(zSlider.value, 10) : 0;

  const zarrUrl = 'https://storage.googleapis.com/nmfs_odp_nwfsc/CB/fish-pace-datasets/chla-z/zarr/';

  return new ZarrLayer({
    id: LAYER_ID,
    source: zarrUrl,
    variable: 'CHLA', 
    zarrVersion: 3, 
    bounds: [-180, -89.98, 180, 89.98], 
    selector: {
      time: { selected: currentTime, type: 'index' }, 
      z: { selected: currentZ, type: 'index' }     
    },
    clim: [0, 13], 
    colormap: [
      '#313695', '#4575b4', '#74add1', '#abd9e9', 
      '#e0f3f8', '#ffffbf', '#fee090', '#fdae61', 
      '#f46d43', '#d73027', '#a50026'
    ]
  });
}

// ==========================================
// 3. Globe Toggle Widget Class
// ==========================================
class GlobeToggleControl {
  constructor(layerFactoryFunction, layerId) {
    this._isGlobe = true; 
    this._createLayer = layerFactoryFunction;
    this._layerId = layerId;
  }

  onAdd(map) {
    this._map = map;
    
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    
    this._button = document.createElement('button');
    this._button.type = 'button';
    this._button.title = 'Switch to Mercator';
    this._button.style.cursor = 'pointer';
    this._button.style.fontSize = '16px';
    this._button.innerHTML = '🌍'; 

    this._button.onclick = () => {
      this._isGlobe = !this._isGlobe;
      
      const layerWasVisible = !!this._map.getLayer(this._layerId);
      
      // Destroy the old layer to free up WebGL memory
      if (layerWasVisible) {
        this._map.removeLayer(this._layerId);
      }

      // Change the projection
      if (this._isGlobe) {
        this._map.setProjection({ type: 'globe' });
        this._button.innerHTML = '🌍';
        this._button.title = 'Switch to Mercator';
      } else {
        this._map.setProjection({ type: 'mercator' });
        this._button.innerHTML = '🗺️';
        this._button.title = 'Switch to Globe';
      }

      // Rebuild the layer once the map finishes transitioning
      if (layerWasVisible) {
        this._map.once('idle', () => {
          if (!this._map.getLayer(this._layerId)) {
            window.zarrLayer = this._createLayer(); 
            this._map.addLayer(window.zarrLayer);
          }
        });
      }
    };
    
    this._container.appendChild(this._button);
    return this._container;
  }
  
  onRemove() {
    if (this._container && this._container.parentNode) {
      this._container.parentNode.removeChild(this._container);
    }
    this._map = undefined;
  }
}


// ==========================================
// Legend Control Widget
// ==========================================
class ColorbarLegendControl {
  onAdd(map) {
    this._map = map;
    
    // Create the main container
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    
    // Style the container to match the dark theme
    Object.assign(this._container.style, {
      backgroundColor: 'rgba(27, 30, 35, 0.9)', // Dark transparent background
      color: '#ffffff', // White text
      padding: '10px',
      borderRadius: '4px',
      fontFamily: 'sans-serif',
      fontSize: '12px',
      boxShadow: '0 0 0 2px rgba(0,0,0,0.1)',
      width: '200px'
    });

    // 1. Add Title
    const title = document.createElement('div');
    title.innerText = 'Chlorophyll-a (mg/m³)';
    title.style.marginBottom = '6px';
    title.style.fontWeight = 'bold';
    title.style.textAlign = 'center';
    this._container.appendChild(title);

    // 2. Add the Color Gradient Bar
    const colorbar = document.createElement('div');
    Object.assign(colorbar.style, {
      height: '12px',
      width: '100%',
      // This gradient exactly matches your ZarrLayer colormap array
      background: 'linear-gradient(to right, #313695, #4575b4, #74add1, #abd9e9, #e0f3f8, #ffffbf, #fee090, #fdae61, #f46d43, #d73027, #a50026)',
      borderRadius: '2px',
      marginBottom: '4px'
    });
    this._container.appendChild(colorbar);

    // 3. Add Min/Max Labels
    const labels = document.createElement('div');
    Object.assign(labels.style, {
      display: 'flex',
      justifyContent: 'space-between',
      opacity: '0.8',
      fontSize: '11px'
    });
    
    const minLabel = document.createElement('span');
    minLabel.innerText = '0'; // Matches your clim min
    
    const maxLabel = document.createElement('span');
    maxLabel.innerText = '13+'; // Matches your clim max
    
    labels.appendChild(minLabel);
    labels.appendChild(maxLabel);
    
    this._container.appendChild(labels);

    return this._container;
  }
  
  onRemove() {
    if (this._container && this._container.parentNode) {
      this._container.parentNode.removeChild(this._container);
    }
    this._map = undefined;
  }
}

// ==========================================
// 4. Add Widget and Initial Layer
// ==========================================
map.addControl(new GlobeToggleControl(createZarrLayer, LAYER_ID), 'top-right');

// Legend
map.addControl(new ColorbarLegendControl(), 'bottom-right');

map.on('load', () => {
  window.zarrLayer = createZarrLayer();
  map.addLayer(window.zarrLayer);
});


// ==========================================
// 5. UI Sliders & Interactivity
// ==========================================
const timeSlider = document.getElementById('time-slider');
const zSlider = document.getElementById('z-slider');
const timeVal = document.getElementById('time-val');
const zVal = document.getElementById('z-val');
const pixelValDisplay = document.getElementById('pixel-val');

const startDate = new Date('2024-03-05T00:00:00Z');

function updateMapData() {
  const currentZIndex = parseInt(zSlider.value, 10);
  const currentTimeIndex = parseInt(timeSlider.value, 10);

  const actualDepth = (currentZIndex * 10) + 5; 
  
  const currentDate = new Date(startDate);
  currentDate.setUTCDate(currentDate.getUTCDate() + currentTimeIndex);
  const actualDate = currentDate.toISOString().split('T')[0];

  zVal.innerText = `${actualDepth} m`;
  timeVal.innerText = actualDate;

  // Make sure we update the global layer, and only if it currently exists
  if (window.zarrLayer) {
    window.zarrLayer.setSelector({
      time: { selected: currentTimeIndex, type: 'index' },
      z: { selected: currentZIndex, type: 'index' }
    });
    map.triggerRepaint();
  }
}

timeSlider.addEventListener('input', updateMapData);
zSlider.addEventListener('input', updateMapData);

// Call immediately to set initial UI text
updateMapData();


// ==========================================
// 6. Map Click Query & Cursor Styling
// ==========================================
map.on('click', async (e) => {
  if (!window.zarrLayer) return;

  const { lng, lat } = e.lngLat;
  pixelValDisplay.innerText = 'Loading...';

  try {
    const result = await window.zarrLayer.queryData({ 
      type: 'Point', 
      coordinates: [lng, lat] 
    });

    if (result && result.CHLA && result.CHLA.length > 0) {
      const chlaValue = result.CHLA[0]; 
      const fillValue = -9999; 
      
      if (chlaValue === fillValue || Number.isNaN(chlaValue)) {
        pixelValDisplay.innerText = 'No data (empty/land)';
      } else {
        pixelValDisplay.innerText = `${chlaValue.toFixed(4)} mg/m³`;
      }
    } else {
      pixelValDisplay.innerText = 'No data';
    }
  } catch (error) {
    console.error('Error querying Zarr layer:', error);
    pixelValDisplay.innerText = 'Query Error';
  }
});

// Cursor pointers
map.on('mouseenter', LAYER_ID, () => {
  map.getCanvas().style.cursor = 'pointer';
});

map.on('mouseleave', LAYER_ID, () => {
  map.getCanvas().style.cursor = '';
});