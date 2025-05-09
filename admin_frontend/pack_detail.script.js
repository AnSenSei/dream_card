// admin_frontend/pack_detail.script.js
document.addEventListener('DOMContentLoaded', () => {
    const packDetailContainer = document.getElementById('packDetailContainer');
    const loadingMessage = document.getElementById('packDetailLoadingMessage');
    const errorMessage = document.getElementById('packDetailErrorMessage');
    const API_BASE_URL = 'http://localhost:8080/gacha/api/v1'; // Match your API base

    async function fetchPackDetails() {
        loadingMessage.style.display = 'block';
        errorMessage.style.display = 'none';
        packDetailContainer.innerHTML = '';

        // Get pack ID from URL query parameter
        const urlParams = new URLSearchParams(window.location.search);
        const packId = urlParams.get('id');

        if (!packId) {
            loadingMessage.style.display = 'none';
            errorMessage.textContent = 'No Pack ID specified in the URL.';
            errorMessage.style.display = 'block';
            return;
        }

        try {
            const response = await fetch(`${API_BASE_URL}/packs/${encodeURIComponent(packId)}`);
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: `Pack not found or HTTP error! Status: ${response.status}` }));
                throw new Error(errorData.detail || `HTTP error! Status: ${response.status}`);
            }
            const pack = await response.json();
            
            loadingMessage.style.display = 'none';
            displayPackDetails(pack);

        } catch (error) {
            loadingMessage.style.display = 'none';
            errorMessage.textContent = `Failed to load pack details: ${error.message}`;
            errorMessage.style.display = 'block';
            console.error('Error fetching pack details:', error);
        }
    }

    function displayPackDetails(pack) {
        let imageHtml = '';
        if (pack.image_url) {
            // Use the signed URL directly
            imageHtml = `<img src="${pack.image_url}" alt="${pack.name}" onerror="this.onerror=null; this.src='#'; this.alt='Image not found';" />`;
        }

        let raritiesHtml = '';
        if (pack.rarity_configurations && Object.keys(pack.rarity_configurations).length > 0) {
            raritiesHtml = '<h3>Rarity Details:</h3><ul>';
            for (const rarityLevel in pack.rarity_configurations) {
                const rarityData = pack.rarity_configurations[rarityLevel];
                // Convert rarityData object to a more readable string or structure
                // For now, just JSON.stringify for simplicity, but this can be improved
                const dataString = JSON.stringify(rarityData, null, 2);
                raritiesHtml += `<li><strong>${rarityLevel}:</strong> <pre>${dataString}</pre></li>`;
            }
            raritiesHtml += '</ul>';
        } else {
            raritiesHtml = '<p>No rarity configurations available for this pack.</p>';
        }

        packDetailContainer.innerHTML = `
            ${imageHtml}
            <h2>${pack.name || 'Unnamed Pack'}</h2>
            <p><strong>ID:</strong> ${pack.id || 'N/A'}</p>
            ${pack.description ? `<p><strong>Description:</strong> ${pack.description}</p>` : ''}
            ${raritiesHtml}
        `;
    }

    // Initial fetch
    fetchPackDetails();
}); 