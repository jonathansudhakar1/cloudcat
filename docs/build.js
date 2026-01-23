import { readFileSync, writeFileSync, readdirSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { Marked } from 'marked';
import hljs from 'highlight.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configure marked with syntax highlighting
const marked = new Marked({
  renderer: {
    code(code, language) {
      const validLang = hljs.getLanguage(language) ? language : 'plaintext';
      const highlighted = hljs.highlight(code, { language: validLang }).value;
      return `<div class="code-block"><pre><code class="hljs language-${validLang}">${highlighted}</code><button class="copy-btn" aria-label="Copy code"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg></button></div>`;
    },
    table(header, body) {
      return `<div class="table-wrapper"><table><thead>${header}</thead><tbody>${body}</tbody></table></div>`;
    }
  }
});

// Content sections in order
const sections = [
  'features',
  'installation',
  'quickstart',
  'command-reference',
  'where-operators',
  'authentication',
  'directory-operations',
  'output-formats',
  'use-cases',
  'performance',
  'troubleshooting',
  'contributing',
  'roadmap'
];

// Read and parse markdown files
function loadContent() {
  const content = {};
  const contentDir = join(__dirname, 'content');

  for (const section of sections) {
    const filePath = join(contentDir, `${section}.md`);
    if (existsSync(filePath)) {
      const md = readFileSync(filePath, 'utf-8');
      content[section] = marked.parse(md);
    } else {
      console.warn(`Warning: ${section}.md not found`);
      content[section] = '';
    }
  }

  return content;
}

// Generate navigation HTML
function generateNav() {
  const navItems = [
    { id: 'features', label: 'Features' },
    { id: 'installation', label: 'Installation' },
    { id: 'quickstart', label: 'Quick Start' },
    { id: 'command-reference', label: 'CLI Reference' },
    { id: 'authentication', label: 'Authentication' },
    { id: 'use-cases', label: 'Use Cases' },
    { id: 'troubleshooting', label: 'Troubleshooting' }
  ];

  return navItems.map(item =>
    `<a href="#${item.id}" class="nav-link">${item.label}</a>`
  ).join('\n');
}

// Generate sitemap
function generateSitemap() {
  const today = new Date().toISOString().split('T')[0];
  const baseUrl = 'https://cloudcatcli.com';

  let sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>${baseUrl}/</loc>
    <lastmod>${today}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>`;

  for (const section of sections) {
    sitemap += `
  <url>
    <loc>${baseUrl}/#${section}</loc>
    <lastmod>${today}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>`;
  }

  sitemap += '\n</urlset>';
  return sitemap;
}

// Main build function
function build() {
  console.log('Building CloudCat documentation...');

  // Load template
  const template = readFileSync(join(__dirname, 'template.html'), 'utf-8');

  // Load content
  const content = loadContent();

  // Generate navigation
  const nav = generateNav();

  // Build sections HTML
  let sectionsHtml = '';
  for (const section of sections) {
    sectionsHtml += `<section id="${section}" class="doc-section">\n${content[section]}\n</section>\n`;
  }

  // Replace placeholders in template
  let html = template
    .replace('{{NAV}}', nav)
    .replace('{{SECTIONS}}', sectionsHtml)
    .replace(/\{\{BUILD_DATE\}\}/g, new Date().toISOString());

  // Write output
  writeFileSync(join(__dirname, 'index.html'), html);
  console.log('Generated index.html');

  // Generate sitemap
  const sitemap = generateSitemap();
  writeFileSync(join(__dirname, 'sitemap.xml'), sitemap);
  console.log('Generated sitemap.xml');

  console.log('Build complete!');
}

build();
