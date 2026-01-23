// CloudCat Documentation - Main JavaScript

(function() {
    'use strict';

    // DOM Elements
    const navToggle = document.getElementById('nav-toggle');
    const nav = document.querySelector('.nav');
    const starCountEl = document.getElementById('star-count');
    const tocLinks = document.querySelectorAll('.toc a');
    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.doc-section');

    // Mobile Navigation Toggle
    if (navToggle) {
        navToggle.addEventListener('click', () => {
            nav.classList.toggle('open');
        });

        // Close nav when clicking a link
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', () => {
                nav.classList.remove('open');
            });
        });
    }

    // Copy to Clipboard
    function initCopyButtons() {
        // Copy buttons in code blocks
        document.querySelectorAll('.code-block .copy-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const code = btn.previousElementSibling.textContent;
                await copyToClipboard(code, btn);
            });
        });

        // Install command copy button
        document.querySelectorAll('.install-command .copy-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const code = btn.dataset.copy || btn.previousElementSibling.textContent;
                await copyToClipboard(code, btn);
            });
        });
    }

    async function copyToClipboard(text, btn) {
        try {
            await navigator.clipboard.writeText(text);
            btn.classList.add('copied');

            // Store original innerHTML
            const originalHTML = btn.innerHTML;
            btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';

            setTimeout(() => {
                btn.classList.remove('copied');
                btn.innerHTML = originalHTML;
            }, 2000);
        } catch (err) {
            console.error('Failed to copy:', err);
        }
    }

    // Smooth Scroll for Anchor Links
    function initSmoothScroll() {
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function(e) {
                e.preventDefault();
                const targetId = this.getAttribute('href').slice(1);
                const target = document.getElementById(targetId);

                if (target) {
                    target.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });

                    // Update URL without scrolling
                    history.pushState(null, null, `#${targetId}`);
                }
            });
        });
    }

    // Active Section Highlighting
    function initScrollSpy() {
        if (!sections.length) return;

        const observerOptions = {
            root: null,
            rootMargin: '-20% 0px -70% 0px',
            threshold: 0
        };

        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const id = entry.target.id;

                    // Update TOC
                    tocLinks.forEach(link => {
                        link.classList.toggle('active', link.getAttribute('href') === `#${id}`);
                    });

                    // Update nav
                    navLinks.forEach(link => {
                        link.classList.toggle('active', link.getAttribute('href') === `#${id}`);
                    });
                }
            });
        }, observerOptions);

        sections.forEach(section => {
            observer.observe(section);
        });
    }

    // Fetch GitHub Star Count
    async function fetchStarCount() {
        if (!starCountEl) return;

        try {
            const response = await fetch('https://api.github.com/repos/jonathansudhakar1/cloudcat');
            if (response.ok) {
                const data = await response.json();
                const count = data.stargazers_count;
                starCountEl.textContent = formatNumber(count);
            }
        } catch (err) {
            console.error('Failed to fetch star count:', err);
        }
    }

    function formatNumber(num) {
        if (num >= 1000) {
            return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
        }
        return num.toString();
    }

    // Add anchor links to headings
    function initHeadingAnchors() {
        document.querySelectorAll('.content h2, .content h3, .content h4').forEach(heading => {
            if (!heading.id) {
                // Generate ID from text content
                const id = heading.textContent
                    .toLowerCase()
                    .replace(/[^a-z0-9]+/g, '-')
                    .replace(/(^-|-$)/g, '');
                heading.id = id;
            }

            // Add anchor link
            const anchor = document.createElement('a');
            anchor.href = `#${heading.id}`;
            anchor.className = 'heading-anchor';
            anchor.innerHTML = '#';
            anchor.setAttribute('aria-hidden', 'true');
            heading.appendChild(anchor);
        });
    }

    // Initialize
    function init() {
        initCopyButtons();
        initSmoothScroll();
        initScrollSpy();
        initHeadingAnchors();
        fetchStarCount();

        // Handle initial hash
        if (window.location.hash) {
            const target = document.querySelector(window.location.hash);
            if (target) {
                setTimeout(() => {
                    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }, 100);
            }
        }
    }

    // Run when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
