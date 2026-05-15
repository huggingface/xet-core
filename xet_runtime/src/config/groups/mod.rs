macro_rules! declare_group_modules {
    ($($group:ident),*) => {
        $(pub mod $group;)*
    };
}
crate::all_config_groups!(declare_group_modules);

pub mod system_monitor;
