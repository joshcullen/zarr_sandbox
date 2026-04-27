import maplibregl from 'maplibre-gl';
import { ZarrLayer } from '@carbonplan/zarr-layer';
import { LayerControl } from 'maplibre-gl-layer-control';
import { Geoman } from '@geoman-io/maplibre-geoman-free';
import * as turf from '@turf/turf';

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

// --------------------------------------------------
// Global handles for debugging
// --------------------------------------------------
window.map = null;
window.zarrLayer = null;
window.geoman = null;

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

async function recalcFeatureMean(featureData) {
  try {
    const geometry = normalizeGeometryForQuery(featureData);

    if (!geometry) {
      console.log('Could not normalize feature:', featureData);
      if (typeof featureData?.getGeoJson === 'function') {
        console.log('Extracted GeoJSON:', featureData.getGeoJson());
      }
      setSidebarText('Unsupported shape for area query');
      return;
    }

    await runZarrQuery(geometry, { isArea: true });
  } catch (err) {
    console.error('Recalculation failed:', err);
    setSidebarText('Area query failed');
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
  map.on('gm:remove', () => {
    console.log('gm:remove');
    clearSidebar();
  });

  updateUI();
});