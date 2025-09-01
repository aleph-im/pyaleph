const isLatestRelease = async () => {
    const q = await fetch('https://api.github.com/repos/aleph-im/pyaleph/releases/latest');
    if(q.ok){
        const res = await q.json();
        return res.tag_name
    }
    throw new Error('Failed to fetch latest release');
}
