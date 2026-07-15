export function SidebarHeader({ brandMark }: { brandMark: string }) {
  return (
    <header className="sidebar-header">
      <div className="brand-lockup">
        <img src={brandMark} alt="Loxa" width="24" height="24" />
      </div>
    </header>
  );
}
