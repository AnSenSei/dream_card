// admin_frontend/add_pack.script.js
document.addEventListener('DOMContentLoaded', () => {
    const addPackForm = document.getElementById('addPackForm');
    const responseMessage = document.getElementById('responseMessage');

    addPackForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        responseMessage.textContent = '';
        responseMessage.className = '';

        const packName = document.getElementById('packName').value.trim();
        const packImageFile = document.getElementById('packImage').files[0]; // Get the file if selected

        if (!packName) {
            responseMessage.textContent = 'Pack Name cannot be empty.';
            responseMessage.className = 'error';
            document.getElementById('packName').focus();
            return;
        }

        let isValid = true;

        // Rarity configurations will be set when adding cards to the pack

        if (!isValid) return;

        // Create FormData
        const formData = new FormData();
        formData.append('pack_name', packName);
        formData.append('collection_id', document.getElementById('collectionId').value.trim());

        // Add win_rate if it exists
        const winRateInput = document.getElementById('winRate');
        if (winRateInput && winRateInput.value.trim()) {
            formData.append('win_rate', winRateInput.value.trim());
        }

        if (packImageFile) {
            formData.append('image_file', packImageFile);
        }

        try {
            const fullApiUrl = 'http://localhost:8080/gacha/api/v1/packs/'; 
            const response = await fetch(fullApiUrl, { 
                method: 'POST',
                // Headers are set automatically by browser for FormData
                // headers: {
                //     'Content-Type': 'application/json', // DO NOT SET THIS for FormData
                // },
                body: formData, // Use formData as body
            });

            const result = await response.json();

            if (response.ok) {
                responseMessage.textContent = `Success: ${result.message} (Pack ID: ${result.pack_id})`;
                responseMessage.className = 'success';
                addPackForm.reset();
                document.getElementById('packImage').value = null; // Clear file input
                // Reset form
            } else {
                responseMessage.textContent = `Error: ${result.detail || 'Failed to add pack. Status: ' + response.status}`;
                responseMessage.className = 'error';
            }
        } catch (error) {
            console.error('Failed to submit form:', error);
            responseMessage.textContent = 'An unexpected error occurred. Check the console for details.';
            responseMessage.className = 'error';
        }
    });
}); 
