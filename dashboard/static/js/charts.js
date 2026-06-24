/* Dashboard GDELT — carga y renderiza gráficos desde MongoDB */

const COLORS = [
    '#3b82f6', '#ef4444', '#22c55e', '#f59e0b', '#8b5cf6',
    '#ec4899', '#06b6d4', '#f97316', '#14b8a6', '#6366f1',
];

const charts = {};

async function fetchData(collection) {
    const resp = await fetch(`/api/${collection}`);
    const json = await resp.json();
    return json.data || [];
}

function makeChart(id, type, labels, datasets, options = {}) {
    const ctx = document.getElementById(id);
    if (!ctx) return;
    if (charts[id]) charts[id].destroy();
    charts[id] = new Chart(ctx, {
        type,
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: { legend: { labels: { color: '#94a3b8' } } },
            scales: type !== 'doughnut' && type !== 'pie' ? {
                x: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
                y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
            } : {},
            ...options,
        },
    });
}

function renderTable(containerId, data, columns) {
    const el = document.getElementById(containerId);
    if (!el || !data.length) {
        if (el) el.innerHTML = '<p style="color:#94a3b8">Sin datos disponibles</p>';
        return;
    }
    let html = '<table><thead><tr>';
    columns.forEach(c => { html += `<th>${c.label}</th>`; });
    html += '</tr></thead><tbody>';
    data.slice(0, 50).forEach(row => {
        html += '<tr>';
        columns.forEach(c => { html += `<td>${row[c.key] ?? ''}</td>`; });
        html += '</tr>';
    });
    html += '</tbody></table>';
    el.innerHTML = html;
}

// --- Tabs ---
document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(btn.dataset.tab).classList.add('active');
    });
});

// --- Load all data ---
async function loadDashboard() {
    try {
        const summary = await fetch('/api/summary').then(r => r.json());
        renderSummary(summary);

        const [
            topCountries, heatmap, conflictPairs, ethnic,
            religion, quadclass, toneCorr, mediaCoverage,
            diversity, sentiment, breaking, escalation,
            actorMatrix, diplomatic, cameo, gkgThemes,
            organizations, lag, hourly,
        ] = await Promise.all([
            fetchData('top_countries_events'),
            fetchData('conflict_heatmap'),
            fetchData('conflict_country_pairs'),
            fetchData('ethnic_conflict_frequency'),
            fetchData('religion_conflict_clusters'),
            fetchData('quadclass_timeline'),
            fetchData('tone_sources_correlation'),
            fetchData('media_coverage'),
            fetchData('source_diversity_index'),
            fetchData('sentiment_trend'),
            fetchData('breaking_news'),
            fetchData('escalation_events'),
            fetchData('actor_interaction_matrix'),
            fetchData('diplomatic_conflict_graph'),
            fetchData('cameo_by_region'),
            fetchData('gkg_themes_by_continent'),
            fetchData('top_organizations'),
            fetchData('tone_lag_analysis'),
            fetchData('hourly_event_density'),
        ]);

        renderTopCountries(topCountries);
        renderHeatmap(heatmap);
        renderConflictPairs(conflictPairs);
        renderEthnic(ethnic);
        renderReligionTable(religion);
        renderQuadclass(quadclass);
        renderToneCorrelation(toneCorr);
        renderMediaCoverage(mediaCoverage);
        renderDiversity(diversity);
        renderSentiment(sentiment);
        renderBreaking(breaking);
        renderEscalation(escalation);
        renderActorMatrix(actorMatrix);
        renderDiplomatic(diplomatic);
        renderCameo(cameo);
        renderGkgThemes(gkgThemes);
        renderOrganizations(organizations);
        renderLag(lag);
        renderHourly(hourly);

        if (summary.last_run) {
            document.getElementById('last-run').textContent = summary.last_run;
        }
    } catch (err) {
        console.error('Error cargando dashboard:', err);
    }
}

function renderSummary(summary) {
    const el = document.getElementById('summary-cards');
    if (!el) return;
    const names = {
        conflict_heatmap: 'Conflictos',
        top_countries_events: 'Top Países',
        actor_interaction_matrix: 'Actores',
        media_coverage: 'Medios',
        breaking_news: 'Breaking News',
        gkg_themes_by_continent: 'Temas GKG',
    };
    let html = '';
    Object.entries(names).forEach(([key, label]) => {
        html += `<div class="card"><div class="count">${summary.collections[key] || 0}</div><div class="label">${label}</div></div>`;
    });
    html += `<div class="card"><div class="count">${summary.total_records || 0}</div><div class="label">Total Registros</div></div>`;
    el.innerHTML = html;
}

function renderTopCountries(data) {
    const latest = {};
    data.forEach(d => {
        const date = d.event_date;
        if (!latest[date] || d.rank < latest[date].rank) latest[date] = d;
    });
    const filtered = data.filter(d => d.rank <= 10);
    const countries = [...new Set(filtered.map(d => d.country_code))].slice(0, 10);
    const counts = countries.map(c => {
        const rows = filtered.filter(d => d.country_code === c);
        return rows.reduce((s, r) => s + (r.event_count || 0), 0);
    });
    makeChart('chart-top-countries', 'bar', countries, [{
        label: 'Eventos', data: counts, backgroundColor: COLORS[0],
    }]);
}

function renderHeatmap(data) {
    const top = data.sort((a, b) => (b.intensity_score || 0) - (a.intensity_score || 0)).slice(0, 15);
    makeChart('chart-heatmap', 'bar', top.map(d => d.country_code), [{
        label: 'Intensidad Goldstein',
        data: top.map(d => Math.abs(d.avg_goldstein || 0)),
        backgroundColor: top.map((_, i) => COLORS[i % COLORS.length]),
    }]);
}

function renderConflictPairs(data) {
    const top = data.slice(0, 10);
    makeChart('chart-conflict-pairs', 'bar',
        top.map(d => `${d.country_1}-${d.country_2}`),
        [{ label: 'Conflictos', data: top.map(d => d.conflict_count), backgroundColor: COLORS[1] }],
    );
}

function renderEthnic(data) {
    const top = data.slice(0, 10);
    makeChart('chart-ethnic', 'doughnut', top.map(d => d.ethnicity), [{
        data: top.map(d => d.conflict_count),
        backgroundColor: COLORS,
    }]);
}

function renderReligionTable(data) {
    renderTable('table-religion', data, [
        { key: 'region', label: 'Región' },
        { key: 'religion', label: 'Religión' },
        { key: 'conflict_count', label: 'Conflictos' },
    ]);
}

function renderQuadclass(data) {
    const regions = [...new Set(data.map(d => d.region))].slice(0, 5);
    const classes = [...new Set(data.map(d => d.QuadClass))].sort();
    const datasets = classes.map((qc, i) => ({
        label: `QuadClass ${qc}`,
        data: regions.map(r => {
            const row = data.find(d => d.region === r && d.QuadClass === qc);
            return row ? row.event_count : 0;
        }),
        backgroundColor: COLORS[i % COLORS.length],
    }));
    makeChart('chart-quadclass', 'bar', regions, datasets, { scales: { x: { stacked: true }, y: { stacked: true } } });
}

function renderToneCorrelation(data) {
    const el = document.getElementById('correlation-info');
    if (!el || !data.length) return;
    const d = data[0];
    el.innerHTML = `
        <div class="value">${(d.correlation || 0).toFixed(4)}</div>
        <div class="detail">Coeficiente de correlación Pearson<br>
        Muestra: ${d.sample_size || 0} eventos<br>
        AvgTone promedio: ${(d.avg_tone || 0).toFixed(2)}<br>
        Fuentes promedio: ${(d.avg_sources || 0).toFixed(2)}</div>`;
}

function renderMediaCoverage(data) {
    const top = data.slice(0, 10);
    makeChart('chart-media-coverage', 'bar', top.map(d => d.country_code), [{
        label: 'Menciones/Evento',
        data: top.map(d => (d.mentions_per_event_ratio || 0).toFixed(2)),
        backgroundColor: COLORS[2],
    }]);
}

function renderDiversity(data) {
    const top = data.slice(0, 10);
    makeChart('chart-diversity', 'bar', top.map(d => d.country_code), [{
        label: 'Índice Diversidad',
        data: top.map(d => (d.diversity_index || 0).toFixed(2)),
        backgroundColor: COLORS[3],
    }]);
}

function renderSentiment(data) {
    const countries = [...new Set(data.map(d => d.country_code))].slice(0, 5);
    const datasets = countries.map((c, i) => {
        const rows = data.filter(d => d.country_code === c).slice(0, 30);
        return {
            label: c,
            data: rows.map(d => d.moving_avg_7d || d.daily_avg_tone),
            borderColor: COLORS[i % COLORS.length],
            fill: false,
            tension: 0.3,
        };
    });
    const labels = data.filter(d => d.country_code === countries[0]).slice(0, 30).map(d => d.event_date);
    makeChart('chart-sentiment', 'line', labels, datasets);
}

function renderBreaking(data) {
    renderTable('table-breaking', data, [
        { key: 'GLOBALEVENTID', label: 'Event ID' },
        { key: 'mention_hour', label: 'Hora' },
        { key: 'hourly_mentions', label: 'Menciones' },
    ]);
}

function renderEscalation(data) {
    renderTable('table-escalation', data, [
        { key: 'GLOBALEVENTID', label: 'Event ID' },
        { key: 'mention_hour', label: 'Hora' },
        { key: 'hourly_mentions', label: 'Menciones' },
        { key: 'growth_rate', label: 'Crecimiento' },
    ]);
}

function renderActorMatrix(data) {
    const top = data.slice(0, 12);
    makeChart('chart-actor-matrix', 'bar',
        top.map(d => `${d.actor1_type}→${d.actor2_type}`),
        [{ label: 'Frecuencia', data: top.map(d => d.frequency), backgroundColor: COLORS[4] }],
    );
}

function renderDiplomatic(data) {
    const dipl = data.filter(d => d.interaction_type === 'diplomatic').slice(0, 8);
    const conf = data.filter(d => d.interaction_type === 'conflict').slice(0, 8);
    makeChart('chart-diplomatic', 'bar',
        [...dipl, ...conf].map(d => `${d.country_1}-${d.country_2}`),
        [
            { label: 'Diplomático', data: dipl.map(d => d.interaction_count), backgroundColor: COLORS[5] },
            { label: 'Conflicto', data: conf.map(d => d.interaction_count), backgroundColor: COLORS[1] },
        ],
    );
}

function renderCameo(data) {
    const regions = [...new Set(data.map(d => d.region))].slice(0, 5);
    const codes = [...new Set(data.map(d => d.EventBaseCode))].slice(0, 8);
    const datasets = codes.map((code, i) => ({
        label: `CAMEO ${code}`,
        data: regions.map(r => {
            const row = data.find(d => d.region === r && d.EventBaseCode === code);
            return row ? row.event_count : 0;
        }),
        backgroundColor: COLORS[i % COLORS.length],
    }));
    makeChart('chart-cameo', 'bar', regions, datasets);
}

function renderGkgThemes(data) {
    const top = data.slice(0, 12);
    makeChart('chart-gkg-themes', 'bar',
        top.map(d => (d.theme_name || '').substring(0, 30)),
        [{ label: 'Frecuencia', data: top.map(d => d.theme_count), backgroundColor: COLORS[6] }],
    );
}

function renderOrganizations(data) {
    const top = data.slice(0, 12);
    makeChart('chart-organizations', 'bar',
        top.map(d => (d.org_name || '').substring(0, 25)),
        [{ label: 'Menciones', data: top.map(d => d.mention_count), backgroundColor: COLORS[7] }],
    );
}

function renderLag(data) {
    const top = data.filter(d => d.lag_correlation != null).slice(0, 10);
    makeChart('chart-lag', 'bar', top.map(d => d.country_code), [{
        label: 'Correlación Rezago',
        data: top.map(d => (d.lag_correlation || 0).toFixed(3)),
        backgroundColor: COLORS[8],
    }]);
}

function renderHourly(data) {
    const labels = data.map(d => `${d.hour_utc}:00`);
    const counts = data.map(d => d.event_count);
    makeChart('chart-hourly', 'line', labels, [{
        label: 'Eventos', data: counts, borderColor: COLORS[0], fill: false, tension: 0.3,
    }]);
    makeChart('chart-hourly-adv', 'bar', labels, [{
        label: 'Eventos por Hora UTC',
        data: counts,
        backgroundColor: COLORS[9],
    }]);
}

document.addEventListener('DOMContentLoaded', loadDashboard);
