import maplibregl from 'maplibre-gl';
import { ZarrLayer } from '@carbonplan/zarr-layer';
import { LayerControl } from 'maplibre-gl-layer-control';
import { Geoman } from '@geoman-io/maplibre-geoman-free';
import * as turf from '@turf/turf';
import shp from 'shpjs'; // Shapefile parser

// Styles
import 'maplibre-gl/dist/maplibre-gl.css';
import '@geoman-io/maplibre-geoman-free/dist/maplibre-geoman.css';

// --------------------------------------------------
// Constants
// --------------------------------------------------
const BASEMAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
const LAYER_ID = 'cmems-data-layer';
const ZARR_SOURCE = 'https://storage.googleapis.com/nmfs_odp_nwfsc/CB/fish-pace-datasets/chla-z/zarr/';
const VARIABLE_NAME = 'CHLA';
const COLORBAR_MIN = 0;
const COLORBAR_MAX = 13;
const TIME_START_DATE = new Date("2024-03-05T00:00:00Z");
const TIME_MAX_INDEX = 559;
const QUERY_UNITS = "mg m⁻³"; // update if needed

// --------------------------------------------------
// Global handles for debugging
// --------------------------------------------------
window.map = null;
window.zarrLayer = null;
window.geoman = null;
window.activeUploadedGeoJSON = null;

// --------------------------------------------------
// DOM references
// --------------------------------------------------
const pixelValDisplay = document.getElementById('pixel-val');
const timeSlider = document.getElementById('time-slider');
const zSlider = document.getElementById('z-slider');
const timeVal = document.getElementById('time-val');
const zVal = document.getElementById('z-val');

// --------------------------------------------------
// Map setup
// --------------------------------------------------
const map = new maplibregl.Map({
  container: 'map',
  style: BASEMAP_STYLE,
  center: [-80, 25],
  zoom: 6,
});

window.map = map;

// --------------------------------------------------
// Custom legend control
// --------------------------------------------------
class ColorbarLegendControl {
  onAdd() {
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl';

    Object.assign(this._container.style, {
      backgroundColor: 'rgba(30, 30, 30, 0.9)',
      color: '#fff',
      padding: '12px',
      borderRadius: '8px',
      width: '180px',
      position: 'relative',
      fontFamily: 'sans-serif',
      boxShadow: '0 2px 4px rgba(0,0,0,0.3)',
    });

    this._container.innerHTML = `
      <div style="font-weight:bold;font-size:11px;margin-bottom:8px">CHLA (mg/m³)</div>
      <div style="height:10px;width:100%;border-radius:2px;background:linear-gradient(to right, #313695, #4575b4, #74add1, #abd9e9, #e0f3f8, #ffffbf, #fee090, #fdae61, #f46d43, #d73027, #a50026)"></div>
      <div
        id="legend-marker"
        style="
          position:absolute;
          top:32px;
          left:12px;
          width:2px;
          height:14px;
          background:#fff;
          border:1px solid #000;
          display:none;
          transition:left 0.2s ease;
          z-index:10;
        "
      ></div>
      <div style="display:flex;justify-content:space-between;margin-top:4px;font-size:10px">
        <span>0</span>
        <span>6.5</span>
        <span>13+</span>
      </div>
    `;

    return this._container;
  }

  onRemove() {
    if (this._container?.parentNode) {
      this._container.parentNode.removeChild(this._container);
    }
  }
}

// --------------------------------------------------
// UI helpers
// --------------------------------------------------
function setSidebarText(text) {
  pixelValDisplay.innerText = text;
}

function clearSidebar() {
  pixelValDisplay.innerText = '--';
  hideLegendMarker();
}

function showLegendMarker(value, min = COLORBAR_MIN, max = COLORBAR_MAX) {
  const marker = document.getElementById('legend-marker');
  if (!marker || !Number.isFinite(value)) return;

  const clamped = Math.max(min, Math.min(max, value));
  const percent = ((clamped - min) / (max - min)) * 100;

  marker.style.display = 'block';
  marker.style.left = `calc(${percent}% + 12px)`;
}

function hideLegendMarker() {
  const marker = document.getElementById('legend-marker');
  if (marker) {
    marker.style.display = 'none';
  }
}

function updateSidebarPoint(mean) {
  pixelValDisplay.innerText = `${mean.toFixed(4)} mg/m³`;
  showLegendMarker(mean);
}

function updateSidebarArea(mean, count) {
  pixelValDisplay.innerHTML = `
    <div style="color: #4ade80; font-weight: bold; font-size: 0.8em; text-transform: uppercase;">
      Area Mean
    </div>
    <div style="font-size: 1.2em;">${mean.toFixed(4)} mg/m³</div>
    <div style="font-size: 0.7em; color: #888;">Pixels: ${count}</div>
  `;
  showLegendMarker(mean);
}

// --------------------------------------------------
// Selector / slider helpers
// --------------------------------------------------
function getCurrentSelector() {
  return {
    time: { selected: parseInt(timeSlider.value, 10), type: 'index' },
    z: { selected: parseInt(zSlider.value, 10), type: 'index' },
  };
}

function updateUI() {
  const tIdx = parseInt(timeSlider.value, 10);
  const zIdx = parseInt(zSlider.value, 10);

  zVal.innerText = `${(zIdx * 10) + 5} m`;

  const date = new Date('2024-03-05T00:00:00Z');
  date.setUTCDate(date.getUTCDate() + tIdx);
  timeVal.innerText = date.toISOString().split('T')[0];

  if (window.zarrLayer) {
    window.zarrLayer.setSelector(getCurrentSelector());
    map.triggerRepaint();
  }

  // Recalculate mean for uploaded shapes if they exist
  if (window.activeUploadedGeoJSON) {
    // Wrap in setTimeout to ensure it runs *after* the Zarr layer updates
    setTimeout(() => {
      recalcFeatureMean(window.activeUploadedGeoJSON);
    }, 50); 
  }
}

timeSlider.addEventListener('input', updateUI);
zSlider.addEventListener('input', updateUI);


//Help with setting up time slider labels
function dateFromTimeIndex(index) {
  const date = new Date(TIME_START_DATE);
  date.setUTCDate(date.getUTCDate() + index);
  return date;
}

function timeIndexFromDate(date) {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.round((date - TIME_START_DATE) / msPerDay);
}

function renderTimeAxis() {
  const svg = document.getElementById("time-axis-svg");
  const slider = document.getElementById("time-slider");

  const sliderRect = slider.getBoundingClientRect();
  const width = sliderRect.width;
  const height = 70;

  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.innerHTML = "";

  const minIndex = Number(slider.min);
  const maxIndex = Number(slider.max);

  const y = 14;

  function xFromIndex(index) {
    return ((index - minIndex) / (maxIndex - minIndex)) * width;
  }

  const axis = document.createElementNS("http://www.w3.org/2000/svg", "line");
  axis.setAttribute("class", "time-axis-highlight");
  axis.setAttribute("x1", xFromIndex(minIndex));
  axis.setAttribute("x2", xFromIndex(maxIndex));
  axis.setAttribute("y1", y);
  axis.setAttribute("y2", y);
  svg.appendChild(axis);

  const firstDate = dateFromTimeIndex(minIndex);
  const lastDate = dateFromTimeIndex(maxIndex);

  const tickDate = new Date(Date.UTC(
    firstDate.getUTCFullYear(),
    firstDate.getUTCMonth(),
    1
  ));

  if (tickDate < firstDate) {
    tickDate.setUTCMonth(tickDate.getUTCMonth() + 1);
  }

  while (tickDate <= lastDate) {
    const index = timeIndexFromDate(tickDate);

    if (index >= minIndex && index <= maxIndex) {
      const x = xFromIndex(index);

      const month = tickDate.toLocaleString("en-US", {
        month: "short",
        timeZone: "UTC",
      });

      const year = tickDate.getUTCFullYear();

      const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
      g.setAttribute("class", "time-tick major");
      g.setAttribute("transform", `translate(${x}, ${y})`);

      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("y2", "10");

      const monthText = document.createElementNS("http://www.w3.org/2000/svg", "text");
      monthText.setAttribute("class", "month-label");
      monthText.setAttribute("y", "26");
      monthText.setAttribute("font-size", "10");
      monthText.textContent = month;

      g.appendChild(line);
      g.appendChild(monthText);

      // Add YEAR label only for January
      if (tickDate.getUTCMonth() === 0) {
        const yearText = document.createElementNS("http://www.w3.org/2000/svg", "text");
        yearText.setAttribute("class", "year-label");
        yearText.setAttribute("y", "55");  // below month
        yearText.setAttribute("font-size", "30");
        yearText.textContent = year;

        g.appendChild(yearText);
      }

      svg.appendChild(g);
    }

    tickDate.setUTCMonth(tickDate.getUTCMonth() + 1);
  }
}

// --------------------------------------------------
// Query helpers
// --------------------------------------------------
const FILL_VALUE = -9999;

const hoverPixelVal = document.getElementById("hover-pixel-val");
const regionMeanVal = document.getElementById("region-mean-val");

function collectNumbers(values, fillValue = FILL_VALUE, depth = 0) {
  if (!values || depth > 10) return [];

  if (Array.isArray(values)) {
    return values.filter((value) =>
      value !== fillValue &&
      typeof value === "number" &&
      Number.isFinite(value)
    );
  }

  if (typeof values !== "object") return [];

  let results = [];
  for (const entry of Object.values(values)) {
    results = results.concat(collectNumbers(entry, fillValue, depth + 1));
  }

  return results;
}

function getQueryStats(result, variableName = VARIABLE_NAME) {
  if (!result || !result[variableName]) {
    return { mean: null, count: 0 };
  }

  const values = collectNumbers(result[variableName]);

  if (!values.length) {
    return { mean: null, count: 0 };
  }

  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;

  return {
    mean: Number.isFinite(mean) ? mean : null,
    count: values.length,
  };
}

function formatQueryStats(stats, includeCount = false) {
  if (!stats || stats.mean == null) return "--";

  const value = `${stats.mean.toFixed(3)} ${QUERY_UNITS}`;

  if (!includeCount) return value;

  return `${value} (${stats.count} pixels)`;
}

function getResultVariableName(result) {
  if (!result || typeof result !== 'object') return null;
  return Object.keys(result).find(
    (key) => key !== 'dimensions' && key !== 'coordinates'
  );
}

function normalizeDataArray(data) {
  if (!data) return [];
  if (Array.isArray(data)) return data;
  if (ArrayBuffer.isView(data)) return Array.from(data);
  return [];
}

function getValidValues(values) {
  return values.filter(
    (v) => v != null && Number.isFinite(v) && v !== -9999
  );
}

function computeMean(values) {
  if (!values.length) return null;
  const sum = values.reduce((acc, v) => acc + v, 0);
  return sum / values.length;
}

function isDrawingInteraction(point) {
  const features = map.queryRenderedFeatures(point);

  return features.some((feature) => {
    const src = feature?.source;
    return typeof src === 'string' && (
      src.includes('geoman') ||
      src.includes('gm_') ||
      src.includes('gm-')
    );
  });
}

function normalizeGeometryForQuery(featureOrGeometry) {
  if (!featureOrGeometry) return null;

  // Handle uploaded FeatureCollections by taking the first feature
  if (featureOrGeometry.type === 'FeatureCollection' && featureOrGeometry.features?.length > 0) {
    return normalizeGeometryForQuery(featureOrGeometry.features[0]);
  }

  // Geoman FeatureData object
  if (typeof featureOrGeometry.getGeoJson === 'function') {
    try {
      const gj = featureOrGeometry.getGeoJson();

      if (gj?.type === 'Feature' && gj.geometry) {
        return normalizeGeometryForQuery(gj);
      }

      if (gj?.type && gj.coordinates) {
        return normalizeGeometryForQuery(gj);
      }
    } catch (err) {
      console.warn('Could not extract GeoJSON from Geoman feature:', err);
    }
  }

  // Plain GeoJSON Feature
  if (featureOrGeometry.type === 'Feature' && featureOrGeometry.geometry) {
    const geometry = featureOrGeometry.geometry;
    const properties = featureOrGeometry.properties || {};

    if (geometry.type === 'Point' && Number.isFinite(properties.radius)) {
      const circlePoly = turf.circle(geometry.coordinates, properties.radius, {
        steps: 64,
        units: 'meters',
      });
      return circlePoly.geometry;
    }

    if (
      geometry.type === 'Point' ||
      geometry.type === 'Polygon' ||
      geometry.type === 'MultiPolygon'
    ) {
      return geometry;
    }

    return null;
  }

  // Raw geometry
  if (
    featureOrGeometry.type === 'Point' ||
    featureOrGeometry.type === 'Polygon' ||
    featureOrGeometry.type === 'MultiPolygon'
  ) {
    return featureOrGeometry;
  }

  // GeometryCollection fallback
  if (
    featureOrGeometry.type === 'GeometryCollection' &&
    Array.isArray(featureOrGeometry.geometries)
  ) {
    const polygonLike = featureOrGeometry.geometries.find(
      (g) => g.type === 'Polygon' || g.type === 'MultiPolygon'
    );
    return polygonLike || null;
  }

  return null;
}

// --------------------------------------------------
// Main query functions
// --------------------------------------------------
async function runZarrQuery(geometry, { isArea = false } = {}) {
  if (!window.zarrLayer || !geometry) return;

  setSidebarText(isArea ? 'Calculating area mean...' : 'Loading...');

  try {
    const result = await window.zarrLayer.queryData(geometry);
    console.log('queryData result:', result);

    const varName = getResultVariableName(result);
    if (!varName) {
      setSidebarText('No variable returned');
      return;
    }

    const rawValues = normalizeDataArray(result[varName]);
    const validValues = getValidValues(rawValues);

    if (!validValues.length) {
      setSidebarText('No valid data in selection');
      if (!isArea) hideLegendMarker();
      return;
    }

    const mean = computeMean(validValues);
    if (!Number.isFinite(mean)) {
      setSidebarText('Mean could not be computed');
      return;
    }

    if (isArea) {
      updateSidebarArea(mean, validValues.length);
    } else {
      updateSidebarPoint(mean);
    }
  } catch (error) {
    console.error('Zarr query failed:', error);
    setSidebarText('Query failed');
    if (!isArea) hideLegendMarker();
  }
}

async function recalcFeatureMean(feature) {
  if (!window.zarrLayer) return;

  const geometry = normalizeGeometryForQuery(feature);

  try {
    const result = await window.zarrLayer.queryData(
      geometry,
      getCurrentSelector(),
      {
        includeSpatialCoordinates: false,
      }
    );

    const stats = getQueryStats(result);
    regionMeanVal.innerText = formatQueryStats(stats, true);
  } catch (error) {
    console.error("Region query failed:", error);
    regionMeanVal.innerText = "--";
  }
}

// --------------------------------------------------
// Point click query
// --------------------------------------------------
map.on('click', (e) => {
  if (isDrawingInteraction(e.point)) return;

  const pointGeometry = {
    type: 'Point',
    coordinates: [e.lngLat.lng, e.lngLat.lat],
  };

  runZarrQuery(pointGeometry, { isArea: false });
});

// --------------------------------------------------
// Load map contents
// --------------------------------------------------
map.on('load', () => {
  map.setProjection({ type: 'globe' });

  // Zarr layer
  window.zarrLayer = new ZarrLayer({
    id: LAYER_ID,
    source: ZARR_SOURCE,
    variable: VARIABLE_NAME,
    zarrVersion: 3,
    dimensions: ['time', 'z', 'lat', 'lon'],
    selector: getCurrentSelector(),
    clim: [COLORBAR_MIN, COLORBAR_MAX],
    colormap: [
      '#313695',
      '#4575b4',
      '#74add1',
      '#abd9e9',
      '#e0f3f8',
      '#ffffbf',
      '#fee090',
      '#fdae61',
      '#f46d43',
      '#d73027',
      '#a50026',
    ],
  });

  map.addLayer(window.zarrLayer);

  // Legend
  map.addControl(new ColorbarLegendControl(), 'bottom-right');

  // Define labels for time slider
  renderTimeAxis();
  updateUI();

  // Update on time slider on page resize
  window.addEventListener("resize", renderTimeAxis);

  // Geoman
  const geoman = new Geoman(map, {
    controlsPosition: 'top-right',
  });

  window.geoman = geoman;

  // Add draw/edit controls
  if (geoman?.controls?.create) {
    geoman.controls.create({
      polygon: true,
      rectangle: true,
      circle: true,
      edit: true,
      delete: true,
    });
  }

  // Force-move the Geoman toolbar into the top-right control container
  requestAnimationFrame(() => {
    const mapContainer = map.getContainer();

    const geomanControls = mapContainer.querySelector('.geoman-controls');
    const topRight = mapContainer.querySelector('.maplibregl-ctrl-top-right');

    if (geomanControls && topRight) {
      topRight.appendChild(geomanControls);
    }
  });

  // Shape created
  map.on('gm:create', async (event) => {
    console.log('gm:create', event);
    await recalcFeatureMean(event.feature);
  });

  // Shape edited (vertex changes, reshaping)
  map.on('gm:edit', async (event) => {
    console.log('gm:edit', event);
    await recalcFeatureMean(event.feature);
  });

  // Shape moved
  map.on('gm:dragend', async (event) => {
    console.log('gm:dragend', event);
    await recalcFeatureMean(event.feature);
  });

  // Broader geometry change hook
  map.on('gm:change', async (event) => {
    console.log('gm:change', event);
    await recalcFeatureMean(event.feature);
  });

  // Shape removed
  map.on("gm:remove", () => {
    regionMeanVal.innerText = "--";
  });

  updateUI();
});


// Query on mouse hover
let hoverQueryEnabled = true;
let hoverQueryInFlight = false;
let lastHoverQueryTime = 0;
let hoverAbortController = null;

const HOVER_QUERY_INTERVAL_MS = 150;

async function queryPointOnHover(event) {
  if (!hoverQueryEnabled || !window.zarrLayer) return;

  const now = Date.now();
  if (hoverQueryInFlight || now - lastHoverQueryTime < HOVER_QUERY_INTERVAL_MS) {
    return;
  }

  lastHoverQueryTime = now;
  hoverQueryInFlight = true;

  if (hoverAbortController) {
    hoverAbortController.abort();
  }

  hoverAbortController = new AbortController();

  const pointGeometry = {
    type: "Point",
    coordinates: [event.lngLat.lng, event.lngLat.lat],
  };

  try {
    const result = await window.zarrLayer.queryData(
      pointGeometry,
      getCurrentSelector(),
      {
        signal: hoverAbortController.signal,
        includeSpatialCoordinates: false,
      }
    );

    const stats = getQueryStats(result);
    hoverPixelVal.innerText = formatQueryStats(stats, false);
  } catch (error) {
    if (error.name !== "AbortError") {
      console.error("Hover query failed:", error);
      hoverPixelVal.innerText = "--";
    }
  } finally {
    hoverQueryInFlight = false;
  }
}

map.on("mousemove", queryPointOnHover);

map.on("mouseleave", () => {
  hoverPixelVal.innerText = "--";
});


// --------------------------------------------------
// Vector File Upload Logic
// --------------------------------------------------

// 1. Create a Custom MapLibre Control for the upload button
class VectorUploadControl {
  onAdd(map) {
    this._map = map;
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    
    // Force this control to clear standard floating
    this._container.style.clear = 'both';

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'custom-upload-btn';
    btn.title = 'Upload Vector File (.geojson, .zip shapefile)';
    btn.onclick = () => document.getElementById('vector-upload').click();
    
    // Explicitly match MapLibre and Geoman button dimensions
    btn.style.width = '29px';
    btn.style.height = '29px';
    btn.style.padding = '0';
    btn.style.display = 'flex';
    btn.style.alignItems = 'center';
    btn.style.justifyContent = 'center';
    btn.style.backgroundColor = '#ffffff';
    btn.style.border = 'none';
    btn.style.cursor = 'pointer';
    
    // Geoman blue (#2371a0) SVG
    btn.innerHTML = `
      <svg viewBox="0 0 24 24" width="18" height="18" style="fill: #2371a0;">
        <path d="M5 20h14v-2H5v2zm7-18L5.33 8.67h4V16h5.34V8.67h4L12 2z"/>
      </svg>
    `;
    
    this._container.appendChild(btn);
    return this._container;
  }
  
  onRemove() {
    this._container.parentNode.removeChild(this._container);
    this._map = undefined;
  }
}

// Create button to clear uploaded vector layer
class ClearUploadControl {
  onAdd(map) {
    this._map = map;
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    this._container.style.clear = 'both'; // Force it to stack cleanly

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'custom-upload-btn'; // Re-use our rounded corners class
    btn.title = 'Remove Uploaded Vector Layer';
    
    btn.style.width = '29px';
    btn.style.height = '29px';
    btn.style.padding = '0';
    btn.style.display = 'flex';
    btn.style.alignItems = 'center';
    btn.style.justifyContent = 'center';
    btn.style.backgroundColor = '#ffffff';
    btn.style.border = 'none';
    btn.style.cursor = 'pointer';

    // Trash SVG
    btn.innerHTML = `
      <svg viewBox="0 0 24 24" width="18" height="18" style="fill: #2371a0;">
        <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
      </svg>
    `;

    // The logic to clear the layer
    btn.onclick = () => {
      if (map.getLayer('uploaded-vector-fill')) map.removeLayer('uploaded-vector-fill');
      if (map.getLayer('uploaded-vector-line')) map.removeLayer('uploaded-vector-line');
      if (map.getSource('uploaded-vector')) map.removeSource('uploaded-vector');
      
      window.activeUploadedGeoJSON = null;
      
      // Reset sidebar display (ensure this ID matches your HTML)
      const meanDisplay = document.getElementById('region-mean-val') || document.getElementById('pixel-val');
      if (meanDisplay) meanDisplay.innerText = '--';
    };

    this._container.appendChild(btn);
    return this._container;
  }
  
  onRemove() {
    this._container.parentNode.removeChild(this._container);
    this._map = undefined;
  }
}


// Add the control to the map (can be placed inside map.on('load'))
map.addControl(new VectorUploadControl(), 'top-right');
map.addControl(new ClearUploadControl(), 'top-right');

// 2. Handle the file selection
const fileInput = document.getElementById('vector-upload');

fileInput.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (!file) return;

  const name = file.name.toLowerCase();
  let geojson = null;

  try {
    regionMeanVal.innerText = 'Parsing file...';
    
    if (name.endsWith('.json') || name.endsWith('.geojson')) {
      const text = await file.text();
      geojson = JSON.parse(text);
    } else if (name.endsWith('.zip')) {
      const buffer = await file.arrayBuffer();
      geojson = await shp(buffer); 
    } else {
      alert('Unsupported format. Please upload .geojson or a zipped Shapefile (.zip).');
      return;
    }

    renderAndQueryUploadedData(geojson);
  } catch (err) {
    console.error("Error parsing uploaded file:", err);
    regionMeanVal.innerText = 'Error parsing file';
  } finally {
    e.target.value = ''; // Reset input to allow re-uploading the same file
  }
});

// 3. Draw on map and calculate mean
function renderAndQueryUploadedData(geojson) {
  window.activeUploadedGeoJSON = geojson;
  const sourceId = 'uploaded-vector';
  
  // Add or update the visual layer on the map
  if (map.getSource(sourceId)) {
    map.getSource(sourceId).setData(geojson);
  } else {
    map.addSource(sourceId, { type: 'geojson', data: geojson });
    map.addLayer({
      id: sourceId + '-fill',
      type: 'fill',
      source: sourceId,
      paint: {
        'fill-color': '#4ade80',
        'fill-opacity': 0.3
      }
    });
    map.addLayer({
      id: sourceId + '-line',
      type: 'line',
      source: sourceId,
      paint: {
        'line-color': '#4ade80',
        'line-width': 2
      }
    });
  }

  // Zoom map to the uploaded features
  try {
    const bbox = turf.bbox(geojson);
    map.fitBounds(bbox, { padding: 40, duration: 1000 });
  } catch (e) {
    console.warn("Could not fit bounds to upload:", e);
  }

  // Calculate mean using your existing query pipeline
  regionMeanVal.innerText = 'Calculating...';
  recalcFeatureMean(geojson);
}