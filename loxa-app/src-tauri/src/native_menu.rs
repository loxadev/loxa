pub(crate) fn with_native_edit_menu<R: tauri::Runtime>(
    builder: tauri::Builder<R>,
) -> tauri::Builder<R> {
    builder.enable_macos_default_menu(false).menu(|app| {
        let undo = tauri::menu::PredefinedMenuItem::undo(app, None)?;
        let redo = tauri::menu::PredefinedMenuItem::redo(app, None)?;
        let separator = tauri::menu::PredefinedMenuItem::separator(app)?;
        let cut = tauri::menu::PredefinedMenuItem::cut(app, None)?;
        let copy = tauri::menu::PredefinedMenuItem::copy(app, None)?;
        let paste = tauri::menu::PredefinedMenuItem::paste(app, None)?;
        let select_all = tauri::menu::PredefinedMenuItem::select_all(app, None)?;
        let edit = tauri::menu::Submenu::with_items(
            app,
            "Edit",
            true,
            &[&undo, &redo, &separator, &cut, &copy, &paste, &select_all],
        )?;
        tauri::menu::Menu::with_items(app, &[&edit])
    })
}
