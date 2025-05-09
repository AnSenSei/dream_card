// admin_frontend/view_packs.script.js
document.addEventListener('DOMContentLoaded', () => {
    const packsContainer = document.getElementById('packsContainer');
    const loadingMessage = document.getElementById('packsLoadingMessage');
    const errorMessage = document.getElementById('packsErrorMessage');
    const API_BASE_URL = 'http://localhost:8080/gacha/api/v1'; // Match your API base

    async function fetchPacks() {
        loadingMessage.style.display = 'block';
        errorMessage.style.display = 'none';
        packsContainer.innerHTML = ''; // Clear previous packs

        try {
            const response = await fetch(`${API_BASE_URL}/packs/`);
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: `HTTP error! Status: ${response.status}` }));
                throw new Error(errorData.detail || `HTTP error! Status: ${response.status}`);
            }
            const packs = await response.json();
            
            loadingMessage.style.display = 'none';
            if (packs && packs.length > 0) {
                displayPacks(packs);
            } else {
                packsContainer.innerHTML = '<p>No packs found.</p>';
            }
        } catch (error) {
            loadingMessage.style.display = 'none';
            errorMessage.textContent = `Failed to load packs: ${error.message}`;
            errorMessage.style.display = 'block';
            console.error('Error fetching packs:', error);
        }
    }

    function displayPacks(packs) {
        packs.forEach(pack => {
            const packCard = document.createElement('div');
            packCard.classList.add('pack-card');

            let imageHtml = '<img src="#" alt="No image available" />'; // Default placeholder
            if (pack.image_url) {
                // Assuming image_url is a direct, publicly accessible URL
                imageHtml = `<img src="${pack.image_url}" alt="${pack.name}" onerror="this.onerror=null; this.src='#'; this.alt='Image not found';" />`;
            }

            packCard.innerHTML = `
                ${imageHtml}
                <h3><a href="pack_detail.html?id=${encodeURIComponent(pack.id)}">${pack.name || 'Unnamed Pack'}</a></h3>
                <p>ID: ${pack.id || 'N/A'}</p>
                ${pack.description ? `<p>Description: ${pack.description}</p>` : ''}
            `;
            packsContainer.appendChild(packCard);
        });
    }

    // Initial fetch
    fetchPacks();
}); 