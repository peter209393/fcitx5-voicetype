#include "voicetype.h"

#include <algorithm>

#include <fcitx-config/iniparser.h>
#include <fcitx-utils/eventdispatcher.h>
#include <fcitx-utils/log.h>
#include <fcitx-utils/utf8.h>
#include <fcitx/addonfactory.h>
#include <fcitx/addonmanager.h>
#include <fcitx/inputpanel.h>
#include <fcitx/text.h>
#include <fcitx/userinterface.h>

using namespace fcitx;

static constexpr char CONF_PATH[] = "conf/voicetype.conf";

VoiceType::VoiceType(Instance *instance) : instance_(instance) {
    reloadConfig();
    handlers_.emplace_back(instance_->watchEvent(
        EventType::InputContextKeyEvent, EventWatcherPhase::PreInputMethod,
        [this](Event &event) { keyEvent(static_cast<KeyEvent &>(event)); }));
    handlers_.emplace_back(instance_->watchEvent(
        EventType::InputContextFocusOut, EventWatcherPhase::Default,
        [this](Event &event) {
            auto &e = static_cast<InputContextEvent &>(event);
            if (session_ && e.inputContext() == ic_.get()) {
                stop();
            }
        }));
}

VoiceType::~VoiceType() { stop(); }

void VoiceType::reloadConfig() {
    readAsIni(config_, CONF_PATH);
    FCITX_INFO() << "voicetype: hotkeys " << Key::keyListToString(*config_.hotkey)
                 << ", key configured: " << std::boolalpha
                 << !(config_.arkApiKey->empty() && config_.speechApiKey->empty());
}

void VoiceType::setConfig(const RawConfig &raw) {
    config_.load(raw, true);
    safeSaveAsIni(config_, CONF_PATH);
}

void VoiceType::keyEvent(KeyEvent &event) {
    const Key &key = event.key();
    const auto &hotkeys = *config_.hotkey;
    if (!event.isRelease()) {
        bool hot = std::any_of(hotkeys.begin(), hotkeys.end(), [&](const Key &k) {
            return k.isModifier() ? key.sym() == k.sym() : key.check(k);
        });
        if (!hot) {
            return;
        }
        if (!session_) {
            start(event.inputContext());
        }
        event.filterAndAccept();
        return;
    }
    // Release: the key sym is enough; modifier state is already changing.
    bool hot = std::any_of(hotkeys.begin(), hotkeys.end(),
                           [&](const Key &k) { return key.sym() == k.sym(); });
    if (!hot) {
        return;
    }
    if (session_ && !finishing_ && event.inputContext() == ic_.get()) {
        finish();
    }
    event.filterAndAccept();
}

void VoiceType::start(InputContext *ic) {
    ic->reset(); // drop any pending composition of the active engine
    ic_ = ic->watch();
    partial_.clear();
    finishing_ = false;
    ctx_ = std::make_unique<Ctx>(Ctx{this, ++gen_});

    VtConfig cfg{config_.arkApiKey->c_str(), config_.speechApiKey->c_str(),
                 config_.hotwords->c_str(), config_.context->c_str(),
                 config_.audioDevice->c_str()};
    session_ = vt_session_start(&cfg, &VoiceType::onCoreEvent, ctx_.get());
    FCITX_INFO() << "voicetype: session started";
    showStatus("🎙", false);
}

void VoiceType::finish() {
    finishing_ = true;
    vt_session_finish(session_);
    showStatus("🎙 …", false);
}

// Releases the core session; safe to call when idle.
void VoiceType::stop() {
    if (!session_) {
        return;
    }
    vt_session_free(session_); // no callbacks after this returns
    session_ = nullptr;
    ctx_.reset();
    finishing_ = false;
    clearPanel();
}

// Runs on the core's worker thread: hop onto the fcitx event loop.
void VoiceType::onCoreEvent(void *user, int kind, const char *text) {
    auto *ctx = static_cast<Ctx *>(user);
    auto *self = ctx->self;
    self->instance_->eventDispatcher().schedule(
        [self, gen = ctx->gen, kind, s = std::string(text ? text : "")] {
            self->handleEvent(gen, kind, s);
        });
}

void VoiceType::handleEvent(uint64_t gen, int kind, const std::string &text) {
    if (gen != gen_ || !session_) {
        return; // stale event from a session that was already stopped
    }
    switch (kind) {
    case VT_EVENT_PARTIAL:
        partial_ = text;
        showPreedit(text);
        break;
    case VT_EVENT_FINAL: {
        const std::string &result = text.empty() ? partial_ : text;
        FCITX_INFO() << "voicetype: final " << result;
        auto *ic = ic_.get();
        stop();
        if (ic && !result.empty()) {
            ic->commitString(result);
        }
        break;
    }
    default:
        FCITX_ERROR() << "voicetype: " << text;
        stop();
        showStatus("⚠ " + text, true);
        break;
    }
}

void VoiceType::showPreedit(const std::string &text) {
    auto *ic = ic_.get();
    if (!ic) {
        return;
    }
    Text preedit(text, TextFormatFlag::Underline);
    preedit.setCursor(text.size());
    auto &panel = ic->inputPanel();
    if (ic->capabilityFlags().test(CapabilityFlag::Preedit)) {
        panel.setClientPreedit(preedit);
    } else {
        panel.setPreedit(preedit);
    }
    ic->updatePreedit();
    ic->updateUserInterface(UserInterfaceComponent::InputPanel);
}

void VoiceType::showStatus(const std::string &text, bool autoClear) {
    auto *ic = ic_.get();
    if (!ic) {
        return;
    }
    ic->inputPanel().setAuxUp(Text(text));
    ic->updateUserInterface(UserInterfaceComponent::InputPanel);
    if (autoClear) {
        clearTimer_ = instance_->eventLoop().addTimeEvent(
            CLOCK_MONOTONIC, now(CLOCK_MONOTONIC) + 4000000, 0,
            [this](EventSourceTime *, uint64_t) {
                clearPanel();
                return true;
            });
    }
}

void VoiceType::clearPanel() {
    clearTimer_.reset();
    auto *ic = ic_.get();
    if (!ic) {
        return;
    }
    ic->inputPanel().reset();
    ic->updatePreedit();
    ic->updateUserInterface(UserInterfaceComponent::InputPanel);
}

class VoiceTypeFactory : public AddonFactory {
public:
    AddonInstance *create(AddonManager *manager) override {
        return new VoiceType(manager->instance());
    }
};

FCITX_ADDON_FACTORY_V2(voicetype, VoiceTypeFactory);
